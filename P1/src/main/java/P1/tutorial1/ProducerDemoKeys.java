package P1.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        String bootstrapServers = "localhost:9092";
        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a producer record
        for (int i = 0; i < 10; i++) {

            String topic = "first-topic";
            String value = "hello world "+i;
            String key = "id_"+ i;
            logger.info("Key: "+key);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            // send data - async
            producer.send(record, (recordMetadata, e) -> {
                // execute when a record is successfully sent or an error is thrown
                if (e == null) {
                    logger.info("\nReceive metadata:" +
                            "\nTopic: " + recordMetadata.topic() +
                            "\nPartition: " + recordMetadata.partition()+
                            "\nOffset: " + recordMetadata.offset()
                    );
                } else {
                    logger.error("Error while producing", e);
                }
            }).get(); // block the send to make it synchronous - really bad, not for prod
        }

        // flush data and close
        producer.close();
    }
}
