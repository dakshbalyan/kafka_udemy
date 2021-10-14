package demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroups {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class.getName()); // .class.getName()

        String bootstrapID = "127.0.0.1:9092";
        String groupID = "fourth_instance";
        String topic = "first_topic";

//      creating consumer config
        Properties propertiesConsumer = new Properties();
        propertiesConsumer.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapID);
        propertiesConsumer.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propertiesConsumer.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propertiesConsumer.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        propertiesConsumer.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

//        creating consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(propertiesConsumer);

//        subscribing to topic
        consumer.subscribe(Arrays.asList(topic));

//        waiting for data
        while(true){ // bad practice to have infinite loop in real life programming, must have break condition
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record : records){
                logger.info("Key: "+record.key()+", Value: " + record.value());
                logger.info("Partition: "+record.partition()+", Offset: "+record.offset());
            }
        }
    }
}
