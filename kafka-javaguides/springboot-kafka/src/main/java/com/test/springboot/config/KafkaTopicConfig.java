package com.test.springboot.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    @Bean
    public NewTopic javaGuidesTopic() {
        //if we write like below then 10 partitions will be created in topic.
        //return TopicBuilder.name("javaguides").partitions(10).build();

        //we are using default partitions provided by kafka
        return TopicBuilder.name("javaguides").build();

    }
}
