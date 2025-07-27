package com.test.kafka.avro;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumerGenericRecord {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "127.0.0.1:9092");
        props.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("group.id", "OrderGroup");
        props.setProperty("schema.registry.url","http://127.0.0.1:8081");
        props.setProperty("specific.avro.reader", "true");


        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("OrderAvroTopic"));
        ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(20));
        for (ConsumerRecord<String, GenericRecord> record : records) {
            String customerName = record.key();
            GenericRecord order = record.value();
            System.out.println("Customer name:" + customerName);
            System.out.println("product:" + order.get("product"));
            System.out.println("quantity:" + order.get("quantity"));
            System.out.println("Message Received...!");
        }
        consumer.close();
    }
}
