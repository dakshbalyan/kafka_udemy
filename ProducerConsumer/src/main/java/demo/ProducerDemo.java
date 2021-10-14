package demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

//        creating Producer properties
        String bootstrapID = "127.0.0.1:9092";
        Properties propertiesProducer = new Properties();
//        Setting up producer configs
        propertiesProducer.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapID);
        propertiesProducer.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propertiesProducer.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        Creating the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(propertiesProducer);
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "This is from IntelliJ4");

//        Sending the data
        producer.send(record);
//        flushing data
        producer.flush();
//        close and flush the producer
        producer.close();
    }
}
