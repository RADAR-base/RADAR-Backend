package org.radarcns.process;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.Arrays;
import java.util.Locale;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.radarcns.config.BatteryStatusConfig;
import org.radarcns.config.ConfigRadar;
import org.radarcns.config.RadarBackendOptions;
import org.radarcns.process.BatteryLevelListener.Status;
import org.radarcns.util.RadarConfig;
import org.radarcns.util.RollingTimeAverage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

/**
 * Monitor a list of topics for anomalous behavior.
 */
public abstract class AbstractKafkaMonitor<K, V> implements KafkaMonitor {
    protected final List<String> topics;
    private static final Logger logger = LoggerFactory.getLogger(AbstractKafkaMonitor.class);

    protected KafkaConsumer consumer;
    private final Properties properties;
    private long pollTimeout;
    private boolean done;

    /**
     * Set some basic properties.
     *
     * Update the properties field in the subclasses. During any overriding constructor, be sure
     * to call {@see configure()}.
     *
     * @param topics topics to monitor
     */
    public AbstractKafkaMonitor(List<String> topics, String clientID) {
        RadarConfig config = RadarConfig.load(RadarConfig.class.getClassLoader());
        properties = new Properties();
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.setProperty(CLIENT_ID_CONFIG, clientID);
        config.updateProperties(properties, SCHEMA_REGISTRY_URL_CONFIG, BOOTSTRAP_SERVERS_CONFIG);

        this.consumer = null;
        this.topics = topics;
        this.pollTimeout = Long.MAX_VALUE;
        this.done = false;
    }

    /**
     * Call to actually create the consumer.
     */
    protected void configure(Properties properties) {
        this.properties.putAll(properties);
        consumer = new KafkaConsumer<>(this.properties);
        consumer.subscribe(topics);
    }

    /**
     * Monitor a given topic until the {@see isShutdown()} method returns true.
     *
     * When a message is encountered that cannot be deserialized,
     * {@link #handleSerializationException()} is called.
     */
    public void start() {
        logger.info("Monitoring streams {}", topics);
        RollingTimeAverage ops = new RollingTimeAverage(20000);

        try {
            while (!isShutdown()) {
                try {
                    ConsumerRecords<K, V> records = consumer.poll(getPollTimeout());
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
     * increases the consumer position to skip that message.
     *
     * The new position is not committed, so on failure of the client, the message must be skipped
     * again.
     */
    // TODO: submit the message to another topic to indicate that it could not be deserialized.
    protected void handleSerializationException() {
        logger.error("Failed to deserialize message. Skipping message.");
        TopicPartition partition = null;
        try {
            Consumer<String, GenericRecord> tmpConsumer = new KafkaConsumer<>(properties);
            for (String topic : topics) {
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
        logger.error("Failed to find faulty message.");
    }

    /** Evaluate a single record that the monitor receives by overriding this function */
    protected abstract void evaluateRecord(ConsumerRecord<K, V> records);

    /** Evaluates the records that the monitor receives */
    protected void evaluateRecords(ConsumerRecords<K, V> records) {
        for (ConsumerRecord<K, V> record : records) {
            evaluateRecord(record);
        }
    }

    /**
     * Whether the monitoring is done.
     *
     * Override to have some stopping behaviour, this implementation always returns false.
     */
    public synchronized boolean isShutdown() {
        return done;
    }

    public synchronized void shutdown() {
        this.done = true;
    }

    public long getPollTimeout() {
        return pollTimeout;
    }

    public void setPollTimeout(long pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    public static KafkaMonitor create(RadarBackendOptions options,
            ConfigRadar properties) {
        BatteryLevelMonitor monitor;
        String[] args = options.getSubCommandArgs();
        String commandType;
        if (args == null || args.length == 0) {
            commandType = "battery";
        } else {
            commandType = args[0];
        }
        switch (commandType) {
            case "battery":
                monitor = new BatteryLevelMonitor("android_empatica_e4_battery_level");
                monitor.addBatteryLevelListener(new BatteryLevelLogger());
                BatteryStatusConfig config = properties.getBatteryStatus();
                if (config != null) {
                    String email = config.getEmailAddress();
                    if (email != null) {
                        Status minStatus = Status.CRITICAL;
                        String level = config.getLevel();
                        if (level != null) {
                            try {
                                minStatus = Status.valueOf(config.getLevel().toUpperCase(Locale.US));
                            } catch (IllegalArgumentException ex) {
                                logger.warn("Do not recognize minimum battery level {}. "
                                                + "Choose from {} instead. Using CRITICAL.",
                                        config.getLevel(), Arrays.toString(Status.values()));
                            }
                        }
                        monitor.addBatteryLevelListener(new BatteryLevelEmail(email, minStatus));
                    }
                }
                break;
            default:
                throw new IllegalArgumentException("Cannot create unknown monitor " + commandType);
        }
        return monitor;
    }
}
