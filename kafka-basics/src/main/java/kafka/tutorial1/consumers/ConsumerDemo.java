package kafka.tutorial1.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    private static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.load(ConsumerDemo.class.getResourceAsStream("/consumer.properties"));

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe consumer to topic(s)
        String topic = "first_topic";
        consumer.subscribe(List.of(topic));
        //poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key: {}, Value: {}, Partition: {}, Offset: {}", record.key(), record.value(), record.partition(), record.offset());
            }
        }
    }
}
