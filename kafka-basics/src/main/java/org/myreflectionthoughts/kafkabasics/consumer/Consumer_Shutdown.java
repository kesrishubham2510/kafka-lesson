package org.myreflectionthoughts.kafkabasics.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

// Demonstrates the ideal way to shut down a kafka-consumer
public class Consumer_Shutdown {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {

        String groupId = "demo-consumers";
        String topicToSubscribe = "kafka-learning-keys";;

        Properties consumerProperties = new Properties();

        consumerProperties.setProperty("bootstrap.servers", "localhost:9092");
        consumerProperties.setProperty("auto.offset.reset", "earliest");
        consumerProperties.setProperty("retries", "3");
        consumerProperties.setProperty("key.deserializer", StringDeserializer.class.getName());
        consumerProperties.setProperty("value.deserializer", StringDeserializer.class.getName());
        consumerProperties.setProperty("group.id", groupId);


        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);

        // Get a reference to the main thread

        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                logger.info("Shutdown detected, exiting....");
                /*
                   Once we invoke wakeup method on the consumer, the next time when kafka consumer polls a topic
                   Wakeup exception is thrown
                */
                kafkaConsumer.wakeup();

                /*
                   allowing main thread to wait until this wakeup exception is handled because
                   we might need to perform certain steps before our program[running on mainThread] can exit,
                 */
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        kafkaConsumer.subscribe(Collections.singleton(topicToSubscribe));

        try {
            while(true){
                logger.info("Polling data from topic");

                kafkaConsumer.poll(Duration.ofSeconds(2)).forEach(record->{
                    logger.info("Partition:- "+record.partition()+", offset:- "+record.offset()+"key:- "+record.key()+" value:- "+ record.value());
                });
            }
        } catch (WakeupException wakeupException) {
            logger.info("Shutdown signal received....");
        } catch (Exception ex){
            logger.info("Exception occurred:- "+ex.getLocalizedMessage());
        } finally{
            // the close method would commit the necessary offsets to kafka
            kafkaConsumer.close();
            logger.info("!! KafkaConsumer is closed !!");
        }

    }
}
