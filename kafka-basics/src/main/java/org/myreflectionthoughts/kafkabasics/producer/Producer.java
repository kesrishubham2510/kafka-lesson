package org.myreflectionthoughts.kafkabasics.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {

    private static final Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());

    public static void main(String[] args) {

        // create producer properties

        Properties producerProperties = new Properties();

        // connecting to the localhost
        producerProperties.setProperty("bootstrap.servers","localhost:9092");

        // set the serializer properties
        producerProperties.setProperty("key.serializer", StringSerializer.class.getName());
        producerProperties.setProperty("value.serializer",StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

        // create a producer record
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("kafka-learning","Hello-Kafjknka");

        // send data
        producer.send(producerRecord);

        // flush the producer
        // tells the producer to send all the data and block until finished
//        producer.flush();

        // also calls producer.flush()
        producer.close();
    }
}
