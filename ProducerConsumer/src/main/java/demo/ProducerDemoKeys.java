package demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
//        creating Producer properties
        String bootstrapID = "127.0.0.1:9092";
        Properties propertiesProducer = new Properties();
//        Setting up producer configs
        propertiesProducer.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapID);
        propertiesProducer.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propertiesProducer.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        Creating the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(propertiesProducer);

        for(int i=1; i<11;i++) {
            String topic = "second_topic";
            String value = "Hi " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic,key,value);

            logger.info("Key : "+key);
//        Sending the data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Sent msg metadata \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while Producing ", e);
                    }
                }
            });
        }
//        flushing data
        producer.flush();
//        close and flush the producer
        producer.close();
    }
}
