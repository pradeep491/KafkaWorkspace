package com.test.springboot.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${spring.kafka.topic.name}")
    private String topicName;
    @Bean
    public NewTopic javaGuidesTopic() {
        //we are using default partitions provided by kafka
        return TopicBuilder.name(topicName).build();

    }
}
