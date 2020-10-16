package kafka.tutorial1.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

    public static void main(String[] args) throws IOException {
        //latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        //create consumer runnable
        logger.info("Creating the consumer runnable");
        ConsumerRunnable myConsumerRunnable = new ConsumerRunnable(latch, "first_topic");

        //start the thread
        Thread myConsumerThread = new Thread(myConsumerRunnable);
        myConsumerThread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            myConsumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException exception) {
                logger.error("Latch got interrupted", exception);
            }
            logger.info("Application has exited");
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted");
        } finally {
            logger.info("Application is closing");
        }
    }

    private static class ConsumerRunnable implements Runnable {

        private final CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        public ConsumerRunnable(CountDownLatch latch, String topic) throws IOException {
            this.latch = latch;
            Properties properties = new Properties();
            properties.load(ConsumerDemoWithThread.class.getResourceAsStream("/consumer.properties"));
            //create consumer
            consumer = new KafkaConsumer<>(properties);

            //subscribe consumer to topic(s)
            //String topic = "first_topic";
            consumer.subscribe(List.of(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    //poll for new data
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: {}, Value: {}, Partition: {}, Offset: {}", record.key(), record.value(), record.partition(), record.offset());
                    }
                }
            } catch (WakeupException exception) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                // tell our main code we're done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }

}

