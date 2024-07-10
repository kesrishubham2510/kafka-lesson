package com.myreflectionthoughts.streamer.handler;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import com.myreflectionthoughts.streamer.Producer.WikimediaChangeProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class WikimediaEventHandler implements EventHandler {

    private static final Logger logger = LoggerFactory.getLogger(WikimediaEventHandler.class);

    private final WikimediaChangeProducer wikimediaChangeProducer;

    public WikimediaEventHandler(WikimediaChangeProducer wikimediaChangeProducer){
        this.wikimediaChangeProducer = wikimediaChangeProducer;
    }

    @Override
    public void onOpen() throws Exception {
        // Nothing to be done
    }

    @Override
    public void onClosed() throws Exception {
        this.wikimediaChangeProducer.getKafkaProducer().close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        logger.info("Pushing the data:- "+messageEvent.getData());
        this.wikimediaChangeProducer.getKafkaProducer().send(new ProducerRecord<String, String>(this.wikimediaChangeProducer.getTopic(), messageEvent.getData()));
    }

    @Override
    public void onComment(String comment) throws Exception {

    }

    @Override
    public void onError(Throwable t) {
        logger.error("Error occured:- "+t.getMessage());

    }
}
