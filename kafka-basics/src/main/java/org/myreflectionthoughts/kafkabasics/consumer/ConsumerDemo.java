package org.myreflectionthoughts.kafkabasics.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {

        String groupId = "demo-consumers";
        String topicToSubscribe = "kafka-learning-keys";;
        Properties consumerProperties = new Properties();

        // connect to kafka server
        consumerProperties.setProperty("bootstrap.servers", "localhost:9092");

        // specifying consumer configs
        consumerProperties.setProperty("retries","3");
        consumerProperties.setProperty("key.deserializer", StringDeserializer.class.getName());
        consumerProperties.setProperty("value.deserializer", StringDeserializer.class.getName());

        consumerProperties.setProperty("group.id", groupId);
        consumerProperties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);

        // subscribe to the topic
        kafkaConsumer.subscribe(Collections.singleton(topicToSubscribe));


        // poll the topic for data
        while(true){
            logger.info("Polling data from topic");

            kafkaConsumer.poll(Duration.ofSeconds(2)).forEach(record->{
                logger.info("Partition:- "+record.partition()+", offset:- "+record.offset()+"key:- "+record.key()+" value:- "+ record.value());
            });
        }


    }
}
