package org.personal.kafka.Stream;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {
    public static void main(String[] args) {
//  Create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo_kafka_streams"); // just like consumer-group ID but here its for
        // streams application
        // the following 2 properties specifies that Strings are taken as keys and values
        // and the method of serialization and deserialization
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // Starting the streams client
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic
        KStream<String , String > inputTopic = streamsBuilder.stream("twitter_tweets");
        // filtering out tweets form users with more than 10k followers
        KStream<String, String> filteredStream = inputTopic.filter(
                (k, jsonTweet) -> extractFollowersFromTweet(jsonTweet) > 10000
        );

        filteredStream.to("filtered_tweets");
        // building the client
        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(), properties);
        // starting the streaming application
        kafkaStreams.start();
    }

    private static JsonParser jsonParser = new JsonParser();
    // to get the id_str out of the tweet data to make the consumer idempotent
    private static Integer extractFollowersFromTweet(String tweetJson){
        // gson library
        try {
            return jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        }catch(NullPointerException e){
            return 0;
        }
    }
}
