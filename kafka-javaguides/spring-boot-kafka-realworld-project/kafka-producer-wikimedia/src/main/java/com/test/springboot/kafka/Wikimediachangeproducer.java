package com.test.springboot.kafka;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
//import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Service
public class Wikimediachangeproducer {
    private static final Logger logger = LoggerFactory.getLogger(Wikimediachangeproducer.class);
    private KafkaTemplate<String, String> template;
    @Value("${spring.kafka.topic.name}")
    private String topicName;
    public Wikimediachangeproducer(KafkaTemplate<String, String> template) {
        this.template = template;
    }

    public void sendMessage() throws InterruptedException {
        //String topic = "wikimedia_recentchange";
        //to read realtime stream data from wikimedia we use Event Source
        EventHandler eventHandler = new WikimediaChangeHandler(template, topicName);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();
        eventSource.start();
        TimeUnit.MINUTES.sleep(10);

    }
}
