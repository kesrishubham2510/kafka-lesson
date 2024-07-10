package com.myreflectionthoughts.streamer.handler;

import com.launchdarkly.eventsource.EventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.net.URI;

@Component
public class EventEngine implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(EventEngine.class);

    private final WikimediaEventHandler wikimediaEventHandler;


    public EventEngine(WikimediaEventHandler wikimediaEventHandler){
        this.wikimediaEventHandler = wikimediaEventHandler;
    }

    @Override
    public void run(String... args) throws Exception {

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(this.wikimediaEventHandler, URI.create(url));
        EventSource eventSource = builder.build();

        logger.info("Starting event consumption...");
        eventSource.start();

    }
}
