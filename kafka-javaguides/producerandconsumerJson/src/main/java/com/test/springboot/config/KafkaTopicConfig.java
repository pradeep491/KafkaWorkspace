package com.test.springboot.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    @Bean
    public NewTopic javaGuidesTopic() {
        //we are using default partitions provided by kafka
        return TopicBuilder.name("javaguides_json").build();

    }
}
