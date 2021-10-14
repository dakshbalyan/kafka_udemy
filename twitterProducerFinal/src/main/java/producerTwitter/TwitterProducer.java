package producerTwitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private final String consumerKey = "MnjNEuMZBEhhLnQ1ajcHp5ADJ";
    private final String consumerSecret = "SyaMcj69iM1nmCs5G0x0MAyu5BziLEdB0KbTvXlzMze13AMcPp";
    private final String token = "3311272854-kk6Gw2xZ8vlvlxaukGBMb4NIMssUlrsVy0cEE8L";
    private final String tokenSecret = "JLcrmVzd9HOc9X1oh69GzcPTf8hYgcErSfWO4kFM4hAYr";

    List<String> terms = Lists.newArrayList("iot", "kafka");

    //    public TwitterProducer(){}
    public static void main(String[] args) {
        new TwitterProducer().initiate();
    }

    public void initiate(){

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100);

        Client client = createTwitterClient(msgQueue);
        client.connect();

        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Stopping application...");
            logger.info("Shutting down client...");
            client.stop();
            logger.info("closing producer...");
            kafkaProducer.close();
            logger.info("DONE !");
        }));

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg!=null){

                kafkaProducer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!=null){
                            logger.error("Something happened during callback!..");
                        }
                    }
                });
                logger.info(msg);
            }
        }
        logger.info("END OF APPLICATION");
    }


    public Client createTwitterClient(BlockingQueue<String> msgQueue) {

        /* Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        // Attempts to establish a connection.
        return hosebirdClient;
    }

    private KafkaProducer<String, String> createKafkaProducer(){
        String bootstrapID = "127.0.0.1:9092";

        Properties propertiesProducer = new Properties();
//        Setting up producer configs
        propertiesProducer.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapID);
        propertiesProducer.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propertiesProducer.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create safe Producer
        propertiesProducer.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // revise
        propertiesProducer.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        propertiesProducer.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        propertiesProducer.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // kafka 2.0 >= 1.1 so we can keep this as 5. Use 1 otherwise.

        // high throughput producer (at the expense of a bit of latency and CPU usage)
        propertiesProducer.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // revise
        propertiesProducer.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        propertiesProducer.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 KB batch size

//        Creating the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(propertiesProducer);

        return producer;
    }

}

// Twitter

// Access token - 3311272854-kk6Gw2xZ8vlvlxaukGBMb4NIMssUlrsVy0cEE8L
// Access token secret - JLcrmVzd9HOc9X1oh69GzcPTf8hYgcErSfWO4kFM4hAYr


// API Key - MnjNEuMZBEhhLnQ1ajcHp5ADJ
// API Secret Key - SyaMcj69iM1nmCs5G0x0MAyu5BziLEdB0KbTvXlzMze13AMcPp