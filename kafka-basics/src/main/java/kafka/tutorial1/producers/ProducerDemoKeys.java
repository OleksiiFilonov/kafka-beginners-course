package kafka.tutorial1.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    private static Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        //create Producer Properties
        Properties properties = new Properties();
        properties.load(ProducerDemoKeys.class.getResourceAsStream("/producer.properties"));

        //create a producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String topic = "first_topic";
        for (int i = 0; i < 10; i++) {
            //create a producer record
            String value = "hello world java " + i;
            String key = "id_" + i;
            logger.info("Key: {}", key);
            //id_0 -> 1
            //id_1 -> 0
            //id_2 -> 2
            //id_3 -> 0
            //id_4 -> 2
            //id_5 -> 2
            //id_6 -> 0
            //id_7 -> 2
            //id_8 -> 1
            //id_9 -> 2
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            //send data - asynchronous
            producer.send(record, (metadata, exception) -> {
                //executes every time record is successfully sent or an exception is thrown
                if (exception == null) {
                    // the record was successfully sent
                    logger.info("Received new metadata\n: Topic: {}\n Partition: {}\n Offset: {}\n Timestamp: {}",
                            metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                } else {
                    logger.error("Error while producing", exception);
                }
            }).get(); // block the .send() to make it synchronous - don't do this in production!
        }
        //flush data
        producer.flush();
        //flush and close producer
        producer.close();
    }
}
