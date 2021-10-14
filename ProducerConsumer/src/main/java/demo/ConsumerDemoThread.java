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
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThread {
    public static void main(String[] args) throws InterruptedException {
        new ConsumerDemoThread().initiate();
    }

    private void initiate() throws InterruptedException {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoThread.class.getName());

        String bootstrapID = "127.0.0.1:9092";
        String groupID = "thread_application";
        String topic = "first_topic";

        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating consumer thread");
        ConsumerThread consumerRunnable = new ConsumerThread(bootstrapID,groupID,topic,latch);

        Thread t = new Thread(consumerRunnable);
        t.start();


        Runtime.getRuntime().addShutdownHook( new Thread( () -> {
         logger.info("Caught in shutdown hook");
         consumerRunnable.shutdown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application exiting");
        }
        ));
        latch.await();
        System.out.println("Application is closing");
    }

    public class ConsumerThread implements Runnable{
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());

        public ConsumerThread( String bootstrapID, String groupID, String topic, CountDownLatch latch){
            this.latch = latch;

            Properties propertiesConsumer = new Properties();
            propertiesConsumer.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapID);
            propertiesConsumer.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            propertiesConsumer.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            propertiesConsumer.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
            propertiesConsumer.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

//        creating consumer
            consumer = new KafkaConsumer<>(propertiesConsumer);

//        subscribing to topic
            consumer.subscribe(Arrays.asList(topic));
        }
        @Override
        public void run() {
            try{
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (Exception e){
                logger.info("Received Shutdown Signal");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }

}
