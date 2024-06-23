package com.test.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;

public class OrderCallback implements org.apache.kafka.clients.producer.Callback {

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        System.out.println(recordMetadata.partition());
        System.out.println(recordMetadata.offset());
        System.out.println("Message Sent successfully");
        if(e != null){
            e.printStackTrace();
        }
    }
}
