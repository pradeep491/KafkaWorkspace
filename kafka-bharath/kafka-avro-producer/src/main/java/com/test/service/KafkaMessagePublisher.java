package com.test.service;

import com.test.avro.Order;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaMessagePublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessageToTopic(Order order) {
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("bharath-avro", order);
        //future.get() //it will slow down the process

        //callback implementation
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent message=[" + order.toString() +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" +
                        order.toString() + "] due to : " + ex.getMessage());
            }
        });
    }
    public void sendGenericRecordToTopic() {
        GenericRecord order = getGenericRecord();
        try {
            kafkaTemplate.send("bharath-avro", order.get("customerName").toString(), order);
            System.out.println("Generic Record Data sent successfully ..");
        }catch(Exception e){
            System.out.println(e.getMessage());
        }
    }

    private static GenericRecord getGenericRecord() {
        Parser parser = new Schema.Parser();
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
        order.put("customerName", "pradeep jasvin");
        order.put("product", 1);
        order.put("quantity", 2);
        return order;
    }
}
