package com.test.kafka;

import com.test.kafka.avro.Order;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class OrderProducerAvro {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "127.0.0.1:9092");
        props.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("schema.registry.url","http://127.0.0.1:8081");
        KafkaProducer<String, Order> producer = new KafkaProducer<>(props);
        Order order = new Order("pradeep1","one plus1",3);
        //can also use setters
        /*order.setCustomerName("pradeep");
        order.setProduct("one plus");
        order.setQuantity(2);*/
        ProducerRecord<String, Order> record = new ProducerRecord<>("OrderAvroTopic",order.getCustomerName().toString(), order);
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
