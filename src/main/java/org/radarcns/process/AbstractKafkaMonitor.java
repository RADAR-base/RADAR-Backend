package org.radarcns.process;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.radarcns.config.BatteryMonitorConfig;
import org.radarcns.config.ConfigRadar;
import org.radarcns.config.DisconnectMonitorConfig;
import org.radarcns.config.MonitorConfig;
import org.radarcns.config.RadarBackendOptions;
import org.radarcns.key.MeasurementKey;
import org.radarcns.util.EmailSender;
import org.radarcns.util.RadarConfig;
import org.radarcns.util.RollingTimeAverage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitor a list of topics for anomalous behavior.
 */
public abstract class AbstractKafkaMonitor<K, V> implements KafkaMonitor {
    protected final Collection<String> topics;
    private static final Logger logger = LoggerFactory.getLogger(AbstractKafkaMonitor.class);

    private KafkaConsumer consumer;
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
    public AbstractKafkaMonitor(Collection<String> topics, String clientId) {
        RadarConfig config = RadarConfig.load(RadarConfig.class.getClassLoader());
        properties = new Properties();
        String deserializer = KafkaAvroDeserializer.class.getName();
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
        properties.setProperty(CLIENT_ID_CONFIG, clientId);
        config.updateProperties(properties, SCHEMA_REGISTRY_URL_CONFIG, BOOTSTRAP_SERVERS_CONFIG);

        this.consumer = null;
        this.topics = topics;
        this.pollTimeout = Long.MAX_VALUE;
        this.done = false;
    }

    /**
     * Call to actually create the consumer.
     */
    protected final void configure(Properties properties) {
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
                    @SuppressWarnings("unchecked")
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
        KafkaMonitor monitor;
        String[] args = options.getSubCommandArgs();
        String commandType;
        if (args == null || args.length == 0) {
            commandType = "battery";
        } else {
            commandType = args[0];
        }
        switch (commandType) {
            case "battery":
                monitor = batteryLevelMonitor(options, properties);
                break;
            case "disconnect":
                monitor = disconnectMonitor(options, properties);
                break;
            default:
                throw new IllegalArgumentException("Cannot create unknown monitor " + commandType);
        }
        return monitor;
    }

    private static KafkaMonitor batteryLevelMonitor(RadarBackendOptions options,
            ConfigRadar properties) {
        BatteryLevelMonitor.Status minLevel = BatteryLevelMonitor.Status.CRITICAL;
        BatteryMonitorConfig config = properties.getBatteryMonitor();
        EmailSender sender = getSender(config);
        Collection<String> topics = getTopics(config, "android_empatica_e4_battery_level");

        if (config != null && config.getLevel() != null) {
            String level = config.getLevel().toUpperCase(Locale.US);
            try {
                minLevel = BatteryLevelMonitor.Status.valueOf(level);
            } catch (IllegalArgumentException ex) {
                logger.warn("Minimum battery level {} is not recognized. "
                                + "Choose from {} instead. Using CRITICAL.",
                        level, Arrays.toString(BatteryLevelMonitor.Status.values()));
            }
        }

        return new BatteryLevelMonitor(topics, sender, minLevel);
    }

    private static KafkaMonitor disconnectMonitor(RadarBackendOptions options,
            ConfigRadar properties) {
        DisconnectMonitorConfig config = properties.getDisconnectMonitor();
        EmailSender sender = getSender(config);
        long timeout = 300_000L;  // 5 minutes
        if (config != null && config.getTimeout() != null) {
            timeout = config.getTimeout();
        }
        Collection<String> topics = getTopics(config, "android_empatica_e4_temperature");
        return new DisconnectMonitor(topics, "temperature_disconnect", sender, timeout);
    }

    private static EmailSender getSender(MonitorConfig config) {
        if (config != null && config.getEmailAddress() != null) {
            return new EmailSender("localhost", "no-reply@radar-cns.org",
                    Collections.singletonList(config.getEmailAddress()));
        }
        return null;
    }

    private static Collection<String> getTopics(MonitorConfig config, String defaultTopic) {
        if (config != null && config.getTopics() != null) {
            return config.getTopics();
        } else {
            return Collections.singleton(defaultTopic);
        }
    }

    protected MeasurementKey extractKey(ConsumerRecord<GenericRecord, ?> record) {
        GenericRecord key = record.key();
        if (key == null) {
            throw new IllegalArgumentException("Failed to process record without a key.");
        }
        Schema keySchema = key.getSchema();
        Field userIdField = keySchema.getField("userId");
        if (userIdField == null) {
            throw new IllegalArgumentException("Failed to process record with key type "
                    + key.getSchema() + " without user ID.");
        }
        Field sourceIdField = keySchema.getField("sourceId");
        if (sourceIdField == null) {
            throw new IllegalArgumentException("Failed to process record with key type "
                    + key.getSchema() + " without source ID.");
        }
        return new MeasurementKey(
                (String) key.get(userIdField.pos()),
                (String) key.get(sourceIdField.pos()));
    }
}
