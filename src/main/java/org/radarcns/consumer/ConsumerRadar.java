package org.radarcns.consumer;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.radarcns.util.KafkaProperties;
import org.radarcns.util.RadarConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public abstract class ConsumerRadar<K, V> implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ConsumerRadar.class);
    public final RadarConfig config;
    protected final KafkaConsumer<K,V> consumer;
    protected final RadarConfig.TopicGroup topics;
    protected final CountDownLatch shutdownLatch;
    protected final Properties properties;

    ConsumerRadar(String clientID, RadarConfig.TopicGroup topics, Properties properties) {
        config = RadarConfig.load(getClass().getClassLoader());
        shutdownLatch = new CountDownLatch(1);

        if (properties == null) {
            properties = KafkaProperties.getSelfCommitConsumer(clientID);
        }
        this.properties = properties;
        consumer = new KafkaConsumer<>(properties);

        if (topics == null) {
            topics = RadarConfig.TopicGroup.all_in;
        }
        this.topics = topics;
    }

    /**
     * Consume a list of Kafka Topics
     */
    public void run() {
        try {
            consumer.subscribe(config.getTopicList(topics));

            while (true) {
                pollMessages();
            }
        } catch (WakeupException e) {
            // ignore, we're closing
        } catch (SerializationException e) {
            handleSerializationError(e);
        } catch (Exception e) {
            handleError(e);
        } finally {
            consumer.close();
            shutdownLatch.countDown();
        }
    }

    protected void handleError(Exception e) {
        log.error("Unexpected error", e);
    }


    /**
     * Handles any deserialization message.
     *
     * This implementation tries to find the partition that contains the faulty message and
     * increases the consumer position to skip that message.
     *
     * The new position is not committed, so on failure of the client, the message must be skipped
     * again.
     */
    // TODO: submit the message to another topic to indicate that it could not be deserialized.
    protected void handleSerializationError(SerializationException e) {
        log.error("Failed to deserialize message. Skipping message.", e);
        TopicPartition partition = null;
        try {
            Consumer<String, GenericRecord> tmpConsumer = new KafkaConsumer<>(properties);
            for (String topic : config.getTopicList(topics)) {
                for (Object partInfo : consumer.partitionsFor(topic)) {
                    partition = new TopicPartition(topic, ((PartitionInfo) partInfo).partition());
                    tmpConsumer.assign(Collections.singletonList(partition));
                    tmpConsumer.seek(partition, consumer.position(partition));
                    tmpConsumer.poll(0);
                }
            }
        } catch (SerializationException ex1) {
            consumer.seek(partition, consumer.position(partition) + 1);
            return;
        }
        log.error("Failed to find faulty message.");
    }

    protected abstract void pollMessages();

    /**
     * Shutdown the consumer and force the commit.
     * Derived classes must override and call through to the super class's implementation of this
     * method.
     */
    public void shutdown() throws InterruptedException {
        consumer.wakeup();
        shutdownLatch.await();
    }
}
