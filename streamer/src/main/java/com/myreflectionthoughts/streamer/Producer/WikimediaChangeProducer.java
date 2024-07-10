package com.myreflectionthoughts.streamer.Producer;

import com.myreflectionthoughts.streamer.configuration.ProducerConfig;
import com.myreflectionthoughts.streamer.constant.ProducerConstant;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class WikimediaChangeProducer {

    private static final Logger logger = LoggerFactory.getLogger(WikimediaChangeProducer.class);
    private final String topic;
    private final KafkaProducer<String, String> kafkaProducer;

    public WikimediaChangeProducer(ProducerConfig producerConfig){

        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConstant.BOOTSTRAP_SERVERS, producerConfig.getBootstrap().getServers());

        // set the serializer properties
        producerProperties.setProperty(ProducerConstant.KEY_SERIALIZER, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConstant.VALUE_SERIALIZER, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConstant.RETRIES, "3");

        this.kafkaProducer = new KafkaProducer<>(producerProperties);
        this.topic = "wikimedia.recentchange";
    }

    public KafkaProducer<String, String> getKafkaProducer() {
        return kafkaProducer;
    }

    public String getTopic() {
        return topic;
    }
}
