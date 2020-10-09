package com.oleksii.filonov.kafka.tutorial2;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterProducer.class);

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.load(TwitterProducer.class.getResourceAsStream("/producer.properties"));
        new TwitterProducer().run(properties);
    }

    private Client createClient(Properties properties, BlockingQueue<String> msgQueue) {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = List.of("kafka");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(
                properties.getProperty("twitter.consumer.key"),
                properties.getProperty("twitter.consumer.secret"),
                properties.getProperty("twitter.token"),
                properties.getProperty("twitter.secret"));

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    public void run(Properties properties) {
        LOGGER.info("Set up");
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100);
        //create Kafka Client
        Client hosebirdClient = createClient(properties, msgQueue);
        // Attempts to establish a connection.
        hosebirdClient.connect();
        //create Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutting down application");
            hosebirdClient.stop();
            LOGGER.info("Stopped Twitter client");
            producer.close();
            LOGGER.info("Closed Kafka Producer");
        }));
        //loop to send tweets to Kafka
        while (!hosebirdClient.isDone()) {
            try {
                String msg = msgQueue.poll(5, TimeUnit.SECONDS);
                LOGGER.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), (metadata, e) -> {
                    if (e != null) {
                        LOGGER.error("Error during sending message to Kafka", e);
                    }
                });
            } catch (InterruptedException e) {
                LOGGER.error("Stopping Twitter client due to ...", e);
                hosebirdClient.stop();
            }
        }
        LOGGER.info("End of application");
    }
}
