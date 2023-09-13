package org.myreflectionthoughts.kafkabasics;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class DemoWithKeys {
    public static void main(String[] args) {

        // conclusion:- same keys always end up in same partition

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
        // pushing bigger batch of messages

        ProducerRecord<String, String> producerRecord;


        for (int i=0; i<5 ; i++) {

        for (int j=0;j<40;j++) {

            String topic = "kafka-learning-keys";
            String key = "id_"+j;
            String message = "message:-"+i+""+j;

            producerRecord = new ProducerRecord<>(topic, key,message);

            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null)
                        System.out.println("Key:- "+key+" Partition:- " + metadata.partition() + " Offset:- " + metadata.offset());
                }
            });

            producer.flush();


        }
            try {
                Thread.sleep(2000);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        producer.close();

    }
}
