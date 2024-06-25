package com.test.kafka.customdeserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class OrderDeserializer implements Deserializer<Order> {

    @Override
    public Order deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        Order order = null;
        try {
            order = mapper.readValue(data, Order.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return order;
    }
}
