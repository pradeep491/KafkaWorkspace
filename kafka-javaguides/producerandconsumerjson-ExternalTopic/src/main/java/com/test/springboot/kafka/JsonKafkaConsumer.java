package com.test.springboot.kafka;

import com.test.springboot.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class JsonKafkaConsumer {
    private static final Logger log = LoggerFactory.getLogger(JsonKafkaConsumer.class);
    @Value("${spring.kafka.topic.name}")
    private String topicName;
    @Value("${spring.kafka.consumer.group-id}")
    private String groupID;

    @KafkaListener(topics = "${spring.kafka.topic.name}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(User user) {
        log.info(String.format("Json Message Received->%s", user.toString()));
    }
}
