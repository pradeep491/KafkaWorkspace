package com.test.springboot.kafka;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
//import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

//public class WikimediaChangeHandler implements BackgroundEventHandler {
public class WikimediaChangeHandler implements EventHandler {
    private static final Logger logger = LoggerFactory.getLogger(WikimediaChangeHandler.class);
    private KafkaTemplate<String, String> template;

    private String topic;

    public WikimediaChangeHandler(KafkaTemplate<String, String> template, String topic) {
        this.template = template;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {

    }

    @Override
    public void onClosed() throws Exception {

    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        logger.info(String.format("Event data->%s", messageEvent.getData()));
        template.send(topic, messageEvent.getData());
    }

    @Override
    public void onComment(String s) throws Exception {

    }

    @Override
    public void onError(Throwable throwable) {

    }
}
