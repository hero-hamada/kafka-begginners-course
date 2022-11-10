package kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

public class ProducerDemoKeys {
    private static final String BOOSTRAP_SERVER = "127.0.0.1:9092";
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemoKeys.class.getName());

    public static void main(String[] args) {
        // create Producer properties https://kafka.apache.org/documentation/#producerconfigs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        IntStream.range(0, 10).forEach(idx -> {
            // create a producer record
            String topic = "first_topic";
            String value = "hello_world" + idx;
            String key = "id_" + idx;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, value, value);
            LOGGER.info("Key: {}", key);
            try {
                producer.send(record, (recordMetadata, e) -> {
                    // executes everytime after record sent or exception is thrown
                    if (Objects.isNull(e)) {
                        // record was successfully sent
                        LOGGER.info("Received new metadata. \nTopics: {}, \nPartition: {}, \nTimestamps: {}",
                                recordMetadata.topic(),
                                recordMetadata.partition(),
                                recordMetadata.timestamp());
                    } else {
                        LOGGER.error("Error while producing", e);
                    }
                }).get(); // block the .send() to make it sync
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        // to be sure that data was sent
        producer.flush();
        producer.close();
    }
}
