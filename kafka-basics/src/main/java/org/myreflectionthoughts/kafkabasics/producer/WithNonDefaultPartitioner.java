package org.myreflectionthoughts.kafkabasics.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class WithNonDefaultPartitioner {

    private static final Logger logger = LoggerFactory.getLogger(WithNonDefaultPartitioner.class.getSimpleName());

    public static void main(String[] args) {

        // create producer properties

        Properties producerProperties = new Properties();

        // connecting to the localhost
        producerProperties.setProperty("bootstrap.servers","localhost:9092");
        producerProperties.setProperty("retries","3");

        // set the serializer properties
        producerProperties.setProperty("key.serializer", StringSerializer.class.getName());
        producerProperties.setProperty("value.serializer",StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

        // create a producer record
        ProducerRecord<String, String> producerRecord;

        // pushing bigger batch of messages

        for (int i=0; i<40 ; i++) {
            producerRecord = new ProducerRecord<>("kafka-learning", "Kafka-with-callbacks:- " + i);

            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null)
                        logger.info("Partition:- " + metadata.partition() + " Offset:- " + metadata.offset());
                }
            });

            producer.flush();

        }
        producer.close();

    }
}