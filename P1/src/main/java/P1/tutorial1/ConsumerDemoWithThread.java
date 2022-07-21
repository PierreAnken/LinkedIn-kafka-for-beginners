package P1.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }


    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
        String bootstrapServers = "localhost:9092";
        String groupId = "my-6th-app";
        String topic = "first-topic";
        logger.info("Creating consumer with thread");
        CountDownLatch latch =  new CountDownLatch(1);


        Runnable myConsumerRunnable = new ConsumerRunnable(groupId, bootstrapServers, topic,latch);
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable)myConsumerRunnable).shutDown();
            try {
                latch.await();
                logger.info("Application exited");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));


        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application is interrupted "+e);
        }finally{
            logger.info("Application is closing");
        }

    }


    public class ConsumerRunnable implements Runnable {

        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        CountDownLatch latch;
        KafkaConsumer<String, String> consumer;


        public ConsumerRunnable(String groupId, String bootstrapServers, String topic, CountDownLatch latch) {
            this.latch = latch;

            // create consumer config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

            //earliest read from start / latest from last one / none = error
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // create a consumer
            consumer = new KafkaConsumer<>(properties);

            // subscribe consumer to our topic(s)
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            // poll for new data
            try {
                while (true) {
                    //consumer.poll(100); // old
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value() + ", Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutDown() {
            consumer.wakeup();
        }
    }
}
