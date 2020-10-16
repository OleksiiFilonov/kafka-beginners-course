package com.oleksii.filonov.kafka.tutorial1.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class ProducerDemoWithCallback {

    private static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) throws IOException {
        //create Producer Properties
        Properties properties = new Properties();
        properties.load(ProducerDemoWithCallback.class.getResourceAsStream("/producer.properties"));

        //create a producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            //create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello_world_java" + i);
            //send data - asynchronous
            producer.send(record, (metadata, exception) -> {
                //executes every time record is successfully sent or an exception is thrown
                if (exception == null) {
                    // the record was successfully sent
                    logger.info("Received new metadata:\nTopic: {}\nPartition: {}\nOffset: {}\nTimestamp: {}",
                            metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                } else {
                    logger.error("Error while producing", exception);
                }
            });
        }
        //flush data
        producer.flush();
        //flush and close producer
        producer.close();
    }
}
