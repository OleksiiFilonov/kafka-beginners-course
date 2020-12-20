package com.oleksii.filonov.kafka.tutorial4;

import com.google.gson.JsonParser;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.io.IOException;
import java.util.Properties;

public class StreamsFilterTweets {

    private static final JsonParser JSON_PARSER = new JsonParser();

    public static void main(String[] args) throws IOException {
        //create properties
        Properties properties = new Properties();
        properties.load(StreamsFilterTweets.class.getResourceAsStream("/streams.properties"));
        //create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> inputTopic = streamsBuilder.stream(properties.getProperty("streams.filter.from"));
        KStream<String, String> filteredTopic = inputTopic.filter((key, jsonTweet) ->
                //filter for tweets which has a user of over 10 000 followers
                extractUserFollowersInTweets(jsonTweet) > 10_000);
        filteredTopic.to(properties.getProperty("streams.filter.to"));
        //build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        //start streams application
        kafkaStreams.start();
    }

    private static int extractUserFollowersInTweets(String jsonTweet) {
        try {
            return JSON_PARSER.parse(jsonTweet).getAsJsonObject()
                    .get("user").getAsJsonObject().get("followers_count").getAsInt();
        } catch (NullPointerException e) {
            return 0;
        }
    }

}
