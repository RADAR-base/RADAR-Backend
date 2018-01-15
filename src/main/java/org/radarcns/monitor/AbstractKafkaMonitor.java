/*
 * Copyright 2017 King's College London and The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.monitor;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.radarcns.config.ConfigRadar;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.util.PersistentStateStore;
import org.radarcns.util.RollingTimeAverage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

/**
 * Monitor a list of topics for anomalous behavior.
 * @param <K> record key type
 * @param <V> record value type
 * @param <S> state type
 */
public abstract class AbstractKafkaMonitor<K, V, S> implements KafkaMonitor {
    private static final Logger logger = LoggerFactory.getLogger(AbstractKafkaMonitor.class);

    protected final Collection<String> topics;
    protected final S state;

    private final PersistentStateStore stateStore;
    private final Properties properties;
    private final AtomicLong pollTimeout;
    private final String groupId;
    private final String clientId;

    private KafkaConsumer consumer;
    private boolean done;

    /**
     * Set some basic properties.
     *
     * <p>Update the properties field in the subclasses. During any overriding constructor, be sure
     * to call {@link #configure(Properties)}.
     * @param radar radar properties
     * @param topics topics to monitor
     * @param groupId Kafka group ID
     * @param clientId Kafka client ID
     * @param stateDefault default state. If null, no state may be used.
     */
    public AbstractKafkaMonitor(RadarPropertyHandler radar, Collection<String> topics,
            String groupId, String clientId, S stateDefault) {

        properties = new Properties();
        String deserializer = KafkaAvroDeserializer.class.getName();
        String monitorClientId = getClass().getName() + "-" + clientId;
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
        properties.setProperty(GROUP_ID_CONFIG, groupId);
        properties.setProperty(CLIENT_ID_CONFIG, monitorClientId);
        properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.setProperty(AUTO_COMMIT_INTERVAL_MS_CONFIG, "1001");
        properties.setProperty(SESSION_TIMEOUT_MS_CONFIG, "15101");
        properties.setProperty(HEARTBEAT_INTERVAL_MS_CONFIG, "7500");

        ConfigRadar config = radar.getRadarProperties();
        properties.setProperty(SCHEMA_REGISTRY_URL_CONFIG, config.getSchemaRegistryPaths());
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, config.getBrokerPaths());

        this.consumer = null;
        this.topics = topics;
        this.pollTimeout = new AtomicLong(Long.MAX_VALUE);
        this.done = false;
        this.clientId = monitorClientId;
        this.groupId = groupId;

        PersistentStateStore localStateStore;
        try {
            localStateStore = radar.getPersistentStateStore();
        } catch (IOException ex) {
            logger.warn("Cannot get persistent state store {}. Not persisting state.",
                    stateDefault.getClass().getName(), ex);
            localStateStore = null;
        }
        this.stateStore = localStateStore;

        S localState = stateDefault;
        if (stateStore != null && stateDefault != null) {
            try {
                localState = stateStore.retrieveState(groupId, monitorClientId, stateDefault);
                logger.info("Using existing {} from persistence store.",
                        stateDefault.getClass().getName());
            } catch (IOException ex) {
                logger.warn("Cannot retrieve persistent state {}. Restarting from empty state.",
                        stateDefault.getClass().getName(), ex);
            }
        } else if (stateDefault != null) {
            logger.info("Persistence path not specified; not retrieving or storing state.");
        }
        state = localState;
    }

    /**
     * Additional configuration to pass to the consumer.
     */
    protected final void configure(Properties properties) {
        this.properties.putAll(properties);
    }

    /**
     * Monitor a given topic until the {@link #isShutdown()} method returns true.
     *
     * <p>When a message is encountered that cannot be deserialized,
     * {@link #handleSerializationException()} is called.
     */
    @Override
    public void start() {
        consumer = new KafkaConsumer<>(this.properties);
        consumer.subscribe(topics);

        logger.info("Monitoring streams {}", topics);
        RollingTimeAverage ops = new RollingTimeAverage(20000);

        try {
            while (!isShutdown()) {
                try {
                    @SuppressWarnings("unchecked")
                    ConsumerRecords<K, V> records = consumer.poll(getPollTimeout());
                    ops.add(records.count());
                    evaluateRecords(records);
                } catch (SerializationException ex) {
                    handleSerializationException();
                } catch (WakeupException ex) {
                    logger.info("Consumer woke up");
                } catch (InterruptException ex) {
                    logger.info("Consumer was interrupted");
                    shutdown();
                } catch (KafkaException ex) {
                    logger.error("Kafka consumer gave exception", ex);
                }
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * Handles any deserialization message.
     *
     * <p>This implementation tries to find the partition that contains the faulty message and
     * increases the consumer position to skip that message.
     *
     * <p>The new position is not committed, so on failure of the client, the message must be
     * skipped again.
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
        if (stateStore != null && state != null) {
            try {
                stateStore.storeState(groupId, clientId, state);
            } catch (IOException ex) {
                logger.error("Failed to store monitor state: {}. "
                        + "When restarted, all current state will be lost.", ex.getMessage());
            }
        }
    }

    /**
     * Whether the monitoring is done.
     *
     * <p>Override to have some stopping behaviour, this implementation always returns false.
     */
    @Override
    public synchronized boolean isShutdown() {
        return done;
    }

    @Override
    public synchronized void shutdown() {
        logger.info("Shutting down monitor {}", getClass().getSimpleName());
        this.done = true;
        this.consumer.wakeup();
    }

    public long getPollTimeout() {
        return pollTimeout.get();
    }

    public void setPollTimeout(long pollTimeout) {
        this.pollTimeout.set(pollTimeout);
    }

    protected ObservationKey extractKey(ConsumerRecord<GenericRecord, ?> record) {
        GenericRecord key = record.key();
        if (key == null) {
            throw new IllegalArgumentException("Failed to process record without a key.");
        }
        Schema keySchema = key.getSchema();
        Field projectIdField = keySchema.getField("projectId");
        if (projectIdField == null) {
            throw new IllegalArgumentException("Failed to process record with key type "
                    + key.getSchema() + " without project ID.");
        }
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
        return new ObservationKey(
                key.get(projectIdField.pos()).toString(),
                key.get(userIdField.pos()).toString(),
                key.get(sourceIdField.pos()).toString());
    }
}
