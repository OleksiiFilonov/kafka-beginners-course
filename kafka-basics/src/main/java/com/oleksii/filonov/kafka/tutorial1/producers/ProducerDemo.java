package com.oleksii.filonov.kafka.tutorial1.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) throws IOException {
        //create Producer Properties
        Properties properties = new Properties();
        properties.load(ProducerDemo.class.getResourceAsStream("/producer.properties"));

        //create a producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello_world_java");
        //send data - asynchronous
        producer.send(record);
        //flush data
        producer.flush();
        //flush and close producer
        producer.close();
    }
}
