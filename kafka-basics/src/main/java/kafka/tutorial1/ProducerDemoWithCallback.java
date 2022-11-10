package kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;
import java.util.stream.IntStream;

public class ProducerDemoWithCallback {
    private static final String BOOSTRAP_SERVER = "127.0.0.1:9092";
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getName());

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
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello_world" + idx);
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
            });
        });

        // to be sure that data was sent
        producer.flush();
        producer.close();
    }
}
