package com.test.kafka.avro;

import com.test.kafka.avro.Order;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "172.28.132.204:9092");
        props.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("group.id", "OrderGroup");
        props.setProperty("schema.registry.url","http://172.28.132.204:8081");
        props.setProperty("specific.avro.reader", "true");


        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("OrderAvroTopic"));
        ConsumerRecords<String, Order> records = consumer.poll(Duration.ofSeconds(20));
        for (ConsumerRecord<String, Order> record : records) {
            String customerName = record.key();
            Order order = record.value();
            System.out.println("Customer name:" + customerName);
            System.out.println("product:" + order.getProduct());
            System.out.println("quantity:" + order.getQuantity());
            System.out.println("Message Received...!");
        }
        consumer.close();
    }
}
