package com.test.kafka.customserializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class OrderSerializer implements Serializer<Order> {


   /* @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }*/

    @Override
    public byte[] serialize(String s, Order order) {
        byte response[] = null;
        ObjectMapper mapper = new ObjectMapper();
        try {
            response = mapper.writeValueAsString(order).getBytes();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return response;
    }

   /* @Override
    public byte[] serialize(String topic, Headers headers, Order data) {
        return Serializer.super.serialize(topic, headers, data);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }*/
}
