package org.radarcns.process;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.radarcns.util.RollingTimeAverage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

/**
 * Monitor a single topic for anomalous behavior.
 */
public abstract class KafkaMonitor {
    protected final String topic;
    private final static Logger logger = LoggerFactory.getLogger(KafkaMonitor.class);

    protected KafkaConsumer consumer;
    private final Properties properties;

    /**
     * Set some basic properties.
     *
     * Update the properties field in the subclasses. During any overriding constructor, be sure
     * to call {@see configure()}.
     *
     * @param topic topic to monitor
     * @param kafkaServers string with Kafka bootstrap servers
     * @param schemaUrl Schema Registry URL
     */
    public KafkaMonitor(String topic, String kafkaServers, String schemaUrl) {
        properties = new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "1");
        properties.setProperty("schema.registry.url", schemaUrl);
        properties.setProperty("bootstrap.servers", kafkaServers);

        this.consumer = null;
        this.topic = topic;
    }

    /**
     * Call to actually create the consumer.
     */
    protected void configure(Properties properties) {
        consumer = new KafkaConsumer<String, GenericRecord>(properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    /**
     * Monitor a given topic until the {@see isDone()} method returns true.
     *
     * When a message is encountered that cannot be deserialized,
     * {@see handleSerializationException} is called.
     */
    public void monitor() {
        logger.info("Monitoring stream {}", topic);
        RollingTimeAverage ops = new RollingTimeAverage(20000);

        try {
            while (!isDone()) {
                try {
                    ConsumerRecords<String, GenericRecord> records = consumer.poll(Long.MAX_VALUE);
                    ops.add(records.count());
                    logger.info("Received {} records", records.count());
                    evaluateRecords(records);
                    consumer.commitSync();
                    logger.debug("Operations per second {}", (int) Math.round(ops.getAverage()));
                } catch (SerializationException ex) {
                    handleSerializationException();
                }
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * Handles any deserialization message.
     *
     * This implementation tries to find the partition that contains the faulty message and
     * increases the consumer position.
     */
    protected void handleSerializationException() {
        logger.error("Failed to deserialize message. Skipping message.");
        TopicPartition partition = null;
        try {
            Consumer<String, GenericRecord> tmpConsumer = new KafkaConsumer<>(properties);
            for (Object partInfo : consumer.partitionsFor(topic)) {
                partition = new TopicPartition(topic, ((PartitionInfo)partInfo).partition());
                tmpConsumer.assign(Collections.singletonList(partition));
                tmpConsumer.seek(partition, consumer.position(partition));
                tmpConsumer.poll(0);
            }
        } catch (SerializationException ex1) {
            consumer.seek(partition, consumer.position(partition) + 1);
            return;
        }
        logger.error("Failed to find faulty message.");
    }

    /** Evaluate the records that the monitor receives by overriding this function */
    protected abstract void evaluateRecords(ConsumerRecords<String, GenericRecord> records);

    /**
     * Whether the monitoring is done.
     *
     * Override to have some stopping behaviour, this implementation always returns false.
     */
    protected boolean isDone() {
        return false;
    }
}
