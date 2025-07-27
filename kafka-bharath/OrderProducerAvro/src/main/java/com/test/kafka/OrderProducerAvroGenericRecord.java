package com.test.kafka;

import com.test.kafka.avro.Order;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class OrderProducerAvroGenericRecord {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "127.0.0.1:9092");
        props.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("schema.registry.url", "http://127.0.0.1:8081");
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);
        Schema.Parser parser = new Schema.Parser();
        GenericRecord order = getGenericRecord();

        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("OrderAvroTopic", order.get("customerName").toString(), order);
        try {
            producer.send(record);
            System.out.println("Message Sent successfully");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static GenericRecord getGenericRecord() {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\n" +
                "  \"namespace\": \"com.test.avro\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"Order\",\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"name\": \"customerName\",\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"product\",\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"quantity\",\n" +
                "      \"type\": \"int\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        GenericRecord order = new GenericData.Record(schema);
        order.put("customerName", "pradeep jyotshna2324");
        order.put("product", "samsung mobile");
        order.put("quantity", 2);
        return order;
    }
}
