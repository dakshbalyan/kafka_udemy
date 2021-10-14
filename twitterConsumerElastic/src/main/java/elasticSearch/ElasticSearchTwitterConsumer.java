package elasticSearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchTwitterConsumer {
    public static RestHighLevelClient createClient(){
        String hostname = "";
        String username = "";
        String password = "";

//        connecting client to the elasticSearch cloud service bonsai
//      setting the endpoint
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));
//      specifying the hostname, port and communication protocol
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic){
        String bootstrapID = "127.0.0.1:9092";
        String groupID = "kafka-elastic-search";

//      creating consumer config
        Properties propertiesConsumer = new Properties();
        propertiesConsumer.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapID);
        propertiesConsumer.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propertiesConsumer.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propertiesConsumer.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        propertiesConsumer.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        propertiesConsumer.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
        propertiesConsumer.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

//        creating consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(propertiesConsumer);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }
    private static JsonParser jsonParser = new JsonParser();
// to get the id_str out of the tweet data to make the consumer idempotent
    private static String extractIdFromTweet(String tweetJson){
        // gson library
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchTwitterConsumer.class.getName());
        RestHighLevelClient client = createClient();
        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        while(true){
//            polling data from the kafka broker to records
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            int recordsCount = records.count();
            logger.info("Received "+ recordsCount + "records");
//          bulk request created to increase throughput
            BulkRequest bulkRequest = new BulkRequest();
//          for every record in records an Index request is created and added to the bulk request with
//            id as id_str
            for(ConsumerRecord<String, String> record : records){
                try{
                    String id = extractIdFromTweet(record.value());

                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets")
                            .source(record.value(), XContentType.JSON).id(id);

                    bulkRequest.add(indexRequest);
                    logger.info("id : "+ id + "added to bulk request");
                }
                catch (NullPointerException e){
                    logger.warn("skipping over bad data: "+ record.value());
                }
            }
//            the bulk request is sent to the elastic search in one go
            if(recordsCount > 0) { // the if condition is there to skip cases where there are no records
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing offsets..");
                consumer.commitAsync(); // manually committing offsets here
                logger.info("offsets commited.");
                try {
                    Thread.sleep(1000);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        }

//        client.close();
    }
}
