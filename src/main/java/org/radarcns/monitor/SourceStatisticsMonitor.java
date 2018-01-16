package org.radarcns.monitor;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.radarcns.config.ConfigRadar;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.SourceStatisticsMonitorConfig;
import org.radarcns.kafka.AggregateKey;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.producer.KafkaSender;
import org.radarcns.producer.KafkaTopicSender;
import org.radarcns.producer.direct.DirectSender;
import org.radarcns.topic.AvroTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.radarcns.util.PersistentStateStore.measurementKeyToString;

/**
 * Monitor a set of streams and compute some basic statistics.
 */
public class SourceStatisticsMonitor extends AbstractKafkaMonitor<GenericRecord, GenericRecord,
        SourceStatisticsMonitor.SourceStatistics> {
    private static final Logger logger = LoggerFactory.getLogger(SourceStatisticsMonitor.class);
    private final AvroTopic<ObservationKey, AggregateKey> outputTopic;
    private final RadarPropertyHandler radar;
    private final long timeout;
    private final int maxSize;
    private long lastEmpty;
    private KafkaSender producer;
    private KafkaTopicSender<ObservationKey, AggregateKey> sender;

    /**
     * Set some basic properties.
     *
     * @param config monitor configuration.
     * @param radar RADAR properties.
     */
    public SourceStatisticsMonitor(RadarPropertyHandler radar,
            SourceStatisticsMonitorConfig config) {
        super(radar, config.getTopics(), Objects.requireNonNull(config.getName(),
                "Source statistics monitor must have a name"), "1", new SourceStatistics());

        this.radar = radar;
        // Group ID based on what persistent state we have.
        // If the persistent state is lost, start from scratch.
        this.outputTopic = new AvroTopic<>(config.getOutputTopic(),
                ObservationKey.getClassSchema(), AggregateKey.getClassSchema(),
                ObservationKey.class, AggregateKey.class);

        Properties props = new Properties();
        props.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(GROUP_ID_CONFIG, state.getGroupId());
        configure(props);

        this.maxSize = config.getMaxBatchSize();
        this.timeout = config.getFlushTimeout();
        this.lastEmpty = state.getUnsent().isEmpty() ? System.currentTimeMillis() : 0L;
    }

    @Override
    public void start() {
        try {
            setupSender();
            super.start();
        } catch (IOException ex) {
            logger.error("Failed to create sender.", ex);
        } finally {
            cleanUpSender();
        }
    }

    protected void cleanUpSender() {
        if (sender != null) {
            try {
                sender.close();
            } catch (IOException e) {
                logger.error("Failed to close sender", e);
            }
        }
        if (producer != null) {
            try {
                producer.close();
            } catch (IOException e) {
                logger.error("Failed to close sender", e);
            }
        }
    }

    protected void setupSender() throws IOException {
        producer = getSender();
        sender = producer.sender(outputTopic);
    }

    @Override
    protected void evaluateRecord(ConsumerRecord<GenericRecord, GenericRecord> entry) {
        GenericRecord key = entry.key();
        GenericRecord value = entry.value();
        if (key == null || value == null) {
            logger.error("Cannot process records on topic {} without both a key and a value",
                    entry.topic());
            return;
        }
        Schema keySchema = key.getSchema();

        ObservationKey newKey;
        try {
            newKey = extractKey(key, keySchema);
        } catch (IllegalArgumentException ex) {
            logger.error("Could not deserialize key from topic {}"
                    + " without projectId, userId or sourceId: {}", entry.topic(), entry.key());
            return;
        }

        Schema valueSchema = value.getSchema();
        double time = getTime(value, valueSchema, "time", Double.NaN);
        time = getTime(value, valueSchema, "timeReceived", time);
        double start = getTime(key, keySchema, "timeStart", time);
        double end = getTime(key, keySchema, "timeEnd", time);

        if (Double.isNaN(start) || Double.isNaN(end)) {
            logger.error("Record in topic {} did not contain time values: <{}, {}>",
                    entry.topic(), entry.key(), entry.value());
            return;
        }

        state.updateSource(newKey, start, end);
    }

    @Override
    protected void evaluateRecords(ConsumerRecords<GenericRecord, GenericRecord> records) {
        super.evaluateRecords(records);

        long now = System.currentTimeMillis();
        Set<ObservationKey> unsent = state.getUnsent();
        if (!unsent.isEmpty() && (now >= lastEmpty + timeout || unsent.size() >= maxSize)) {
            // send all entries and remove them from the batch only if successful.
            unsent.removeIf(key -> {
                try {
                    AggregateKey value = state.getSource(key);
                    sender.send(key, value);
                    return true;
                } catch (Exception ex) {
                    logger.error("Failed to update state for observation {}",
                            key, ex);
                    return false;
                }
            });
        }

        // either it was empty before or all records were sent
        if (unsent.isEmpty()) {
            lastEmpty = now;
        }
    }

    private static double getTime(GenericRecord record, Schema schema, String fieldName,
            double defaultValue) {
        Schema.Field field = schema.getField(fieldName);
        if (field != null) {
            return ((Number) record.get(field.pos())).doubleValue();
        } else {
            return defaultValue;
        }
    }

    protected KafkaSender getSender() {
        Properties properties = new Properties();
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(GROUP_ID_CONFIG, state.getGroupId() + "_producers");
        properties.setProperty(CLIENT_ID_CONFIG, getClass().getName() + "-1");
        properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.setProperty(AUTO_COMMIT_INTERVAL_MS_CONFIG, "1001");
        properties.setProperty(SESSION_TIMEOUT_MS_CONFIG, "15101");
        properties.setProperty(HEARTBEAT_INTERVAL_MS_CONFIG, "7500");

        ConfigRadar config = radar.getRadarProperties();
        properties.setProperty(SCHEMA_REGISTRY_URL_CONFIG, config.getSchemaRegistryPaths());
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, config.getBrokerPaths());

        return new DirectSender(properties);
    }

    public static class SourceStatistics {
        private final Map<String, AggregateKey> sources = new HashMap<>();
        private final Set<ObservationKey> unsent = new HashSet<>();
        private String groupId = UUID.randomUUID().toString();

        public AggregateKey getSource(ObservationKey key) {
            return this.sources.get(measurementKeyToString(key));
        }

        public Map<String, AggregateKey> getSources() {
            return sources;
        }

        public void setSources(Map<String, AggregateKey> sources) {
            this.sources.putAll(sources);
        }

        public void updateSource(ObservationKey key, double start, double end) {
            sources.compute(measurementKeyToString(key), (k, v) -> {
                if (v == null) {
                    return new AggregateKey(
                            key.getProjectId(), key.getUserId(), key.getSourceId(),
                            start, end);
                } else {
                    if (v.getTimeStart() > start) {
                        v.setTimeStart(start);
                    }
                    if (v.getTimeEnd() < end) {
                        v.setTimeEnd(end);
                    }
                    return v;
                }
            });
            unsent.add(key);
        }

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public Set<ObservationKey> getUnsent() {
            return unsent;
        }

        public void setUnsent(Set<ObservationKey> unsent) {
            this.unsent.addAll(unsent);
        }
    }
}
