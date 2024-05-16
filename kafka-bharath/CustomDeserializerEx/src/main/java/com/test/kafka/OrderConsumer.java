package com.test.kafka;

import com.test.kafka.customdeserializer.Order;
import com.test.kafka.customdeserializer.OrderDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", OrderDeserializer.class.getName());
        props.setProperty("group.id", "OrderGroup");

        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("OrderCSTopic"));
        ConsumerRecords<String, Order> records = consumer.poll(Duration.ofSeconds(20));
        for (ConsumerRecord<String, Order> record : records) {
            String customerName = record.key();
            Order order = record.value();
            System.out.println("Customer name:"+customerName);
            System.out.println("product:"+order.getProduct());
            System.out.println("quantity:"+order.getQuantity());
        }
        consumer.close();
    }
}
