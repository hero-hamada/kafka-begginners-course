package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
    private static final String BOOSTRAP_SERVER = "127.0.0.1:9092";
    private static final String GROUP_ID = "my-sixth-app";
    private static final String TOPIC = "first_topic";

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread() {
    }

    private void run() {
        // latch dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        // start the thread
        ConsumerRunnable consumerRunnable = new ConsumerRunnable(
                BOOSTRAP_SERVER,
                GROUP_ID,
                TOPIC,
                latch);
        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Caught shutdown hook");
            consumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            LOGGER.info("Application exited");
        }
        ));
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOGGER.info("Application got interrupted", e);
        } finally {
            LOGGER.info("Application closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String bootstrapServers,
                                String groupId,
                                String topic,
                                CountDownLatch latch) {
            this.latch = latch;
            // create consumer config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            // create consumer
            consumer = new KafkaConsumer<>(properties);
            //subscribe consumer to our topics
            consumer.subscribe(Collections.singletonList(topic));
        }

        @Override
        public void run() {
            // poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    records.forEach(record -> LOGGER.info("Key: {}, Value: {} \n" +
                            "Partition: {}, Offset: {}", record.key(), record.value(), record.partition(), record.offset()));

                }
            } catch (WakeupException e) {
                LOGGER.info("Received shutdown signal!");
            } finally {
                consumer.close();
                // tell main code consumer is done
                latch.countDown();
            }
        }

        public void shutdown() {
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeupException
            consumer.wakeup();
        }
    }
}
