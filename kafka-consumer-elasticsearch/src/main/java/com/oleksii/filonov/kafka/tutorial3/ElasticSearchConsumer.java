package com.oleksii.filonov.kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ElasticSearchConsumer {

    JsonParser parser = new JsonParser();

    private static Logger LOGGER = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    private RestHighLevelClient createElastiClient(Properties prop) {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(
                        prop.getProperty("elasticsearch.user"),
                        prop.getProperty("elasticsearch.password")));

        RestClientBuilder clientBuilder = RestClient.builder(
                new HttpHost(prop.getProperty("elasticsearch.hostname"),
                        Integer.parseInt(prop.getProperty("elasticsearch.port")),
                        prop.getProperty("elasticsearch.schema")))
                .setHttpClientConfigCallback(builder -> builder.setDefaultCredentialsProvider(credentialsProvider));
        return new RestHighLevelClient(clientBuilder);
    }

    private KafkaConsumer<String, String> createKafkaConsumer(Properties properties) {
        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe consumer to topic(s)
        consumer.subscribe(List.of(properties.getProperty("kafka.topic")));
        return consumer;

    }

    public static void main(String[] args) throws IOException{
        Properties properties = new Properties();
        properties.load(ElasticSearchConsumer.class.getResourceAsStream("/consumer.properties"));
        ElasticSearchConsumer esConsumer = new ElasticSearchConsumer();
        RestHighLevelClient client = esConsumer.createElastiClient(properties);
        var consumer = esConsumer.createKafkaConsumer(properties);
        //poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            final int recordsCount = records.count();
            LOGGER.info("Received {} records", recordsCount);
            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record : records) {
                //insert data into ES
                bulkRequest.add(
                        new IndexRequest("twitter")
                                .id(esConsumer.extractIdFromTweet(record.value()))
                                .source(record.value(), XContentType.JSON));
            }
            if (recordsCount > 0) {
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                LOGGER.debug("Send {} items to ES in bulk", bulkResponse.getItems().length);
            }
            LOGGER.info("Committing offsets ...");
            consumer.commitSync();
            LOGGER.info("Offsets have been committed");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOGGER.error("While Loop Sleep interrupted", e);
            }
        }
        //close Elastic Search client gracefully

    }

   private String extractIdFromTweet(String jsonTweet) {
       return parser.parse(jsonTweet).getAsJsonObject().get("id_str").getAsString();
    }
}
