package org.radarcns.stream;

import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.radarcns.monitor.AbstractKafkaMonitor.extractKey;

import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.RadarPropertyHandler.Priority;
import org.radarcns.config.SourceStatisticsMonitorConfig;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.monitor.KafkaMonitor;
import org.radarcns.util.serde.RadarSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceStatisticsStream implements KafkaMonitor {
    private static final Logger logger = LoggerFactory.getLogger(SourceStatisticsStream.class);

    private final RadarPropertyHandler properties;
    private final List<String> inputTopics;
    private final String outputTopic;
    private final String name;
    private volatile long interval;
    private KafkaStreams stream;
    private volatile boolean isShutdown;

    public SourceStatisticsStream(RadarPropertyHandler properties, SourceStatisticsMonitorConfig config) {
        this.properties = properties;
        this.inputTopics = config.getTopics();
        this.outputTopic = config.getOutputTopic();
        this.name = config.getName();
        this.interval = config.getFlushTimeout();
        this.isShutdown = false;
    }

    public void start() {
        stream = new KafkaStreams(getTopology(), getStreamsConfig());
        stream.setUncaughtExceptionHandler((thread, ex) -> shutdown());
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown, "Shutdown-Streams"));
        stream.start();
    }

    @Override
    public void shutdown() {
        isShutdown = true;
        stream.close();
    }

    private Topology getTopology() {
        Topology builder = new Topology();
        GenericAvroDeserializer genericReader = new GenericAvroDeserializer();

        StoreBuilder<KeyValueStore<ObservationKey, SourceStatisticsRecord>> statisticsStore = Stores
                .keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("statistics"),
                        new SpecificAvroSerde<>(),
                        new RadarSerde<>(SourceStatisticsRecord.class).getSerde());

        builder.addSource("source", genericReader, genericReader, inputTopics.toArray(new String[0]));
        builder.addProcessor("process", SourceStatisticsProcessor::new, "source");
        builder.addSink("sink", outputTopic,
                new SpecificAvroSerializer<ObservationKey>(),
                new SpecificAvroSerializer<SourceStatistics>(),
                "process");

        builder.addStateStore(statisticsStore, "process");
        return builder;
    }

    private StreamsConfig getStreamsConfig() {
        Properties settings = properties.getKafkaProperties().getStreamProperties(name,
                properties.getRadarProperties().threadsByPriority(Priority.HIGH, 3));
        settings.remove(DEFAULT_KEY_SERDE_CLASS_CONFIG);
        settings.remove(DEFAULT_VALUE_SERDE_CLASS_CONFIG);
        return new StreamsConfig(settings);
    }

    @Override
    public boolean isShutdown() {
        return isShutdown;
    }

    @Override
    public long getPollTimeout() {
        return interval;
    }

    @Override
    public void setPollTimeout(long pollTimeout) {
        this.interval = pollTimeout;
    }

    private class SourceStatisticsProcessor implements Processor<GenericRecord, GenericRecord> {
        private KeyValueStore<ObservationKey, SourceStatisticsRecord> store;
        private ProcessorContext context;
        private Cancellable punctuateCancellor;
        private long localInterval = -1L;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            store = (KeyValueStore<ObservationKey, SourceStatisticsRecord>) context.getStateStore("statistics");
            this.context = context;
            updatePunctuate();
        }

        private void updatePunctuate() {
            if (localInterval != getPollTimeout()) {
                localInterval = getPollTimeout();
                if (punctuateCancellor != null) {
                    punctuateCancellor.cancel();
                }
                punctuateCancellor = this.context.schedule(
                        localInterval, PunctuationType.WALL_CLOCK_TIME, this::sendNew);
            }
        }

        @SuppressWarnings("unused")
        private void sendNew(long timestamp) {
            List<KeyValue<ObservationKey, SourceStatisticsRecord>> sent = new ArrayList<>();

            try (KeyValueIterator<ObservationKey, SourceStatisticsRecord> iterator = store.all()) {
                while (iterator.hasNext()) {
                    KeyValue<ObservationKey, SourceStatisticsRecord> next = iterator.next();
                    if (!next.value.isSent) {
                        context.forward(next.key, next.value.sourceStatistics());
                        sent.add(new KeyValue<>(next.key, next.value.sentRecord()));
                    }
                }
            }

            sent.forEach(e -> store.put(e.key, e.value));
            context.commit();

            updatePunctuate();
        }

        @Override
        public void process(GenericRecord genericKey, GenericRecord value) {
            if (genericKey == null || value == null) {
                logger.error("Cannot process records without both a key and a value");
                return;
            }
            Schema keySchema = genericKey.getSchema();

            Schema valueSchema = value.getSchema();
            double time = getTime(value, valueSchema, "time", Double.NaN);
            time = getTime(value, valueSchema, "timeReceived", time);
            double timeStart = getTime(genericKey, keySchema, "timeStart", time);
            double timeEnd = getTime(genericKey, keySchema, "timeEnd", time);

            if (Double.isNaN(timeStart) || Double.isNaN(timeEnd)) {
                logger.error("Record did not contain time values: <{}, {}>", genericKey, value);
                return;
            }

            ObservationKey key;
            try {
                key = extractKey(genericKey, keySchema);
            } catch (IllegalArgumentException ex) {
                logger.error("Could not deserialize key"
                        + " without projectId, userId or sourceId: {}", genericKey);
                return;
            }

            SourceStatisticsRecord stats = store.get(key);
            SourceStatisticsRecord newStats = SourceStatisticsRecord.updateRecord(stats, timeStart, timeEnd);
            if (newStats != stats) {
                store.put(key, newStats);
            }
        }

        @Override
        public void punctuate(long timestamp) {
            // deprecated
        }

        @Override
        public void close() {
            // do nothing
        }
    }

    public static class SourceStatisticsRecord {
        private final double timeStart;
        private final double timeEnd;
        private final boolean isSent;

        @JsonCreator
        public SourceStatisticsRecord(
                @JsonProperty("timeStart") double timeStart,
                @JsonProperty("timeEnd") double timeEnd,
                @JsonProperty("isSent") boolean isSent) {
            this.timeStart = timeStart;
            this.timeEnd = timeEnd;
            this.isSent = isSent;
        }

        public SourceStatistics sourceStatistics() {
            return new SourceStatistics(timeStart, timeEnd);
        }

        static SourceStatisticsRecord updateRecord(SourceStatisticsRecord old,
                double timeStart, double timeEnd) {
            if (old == null) {
                return new SourceStatisticsRecord(timeStart, timeEnd, false);
            } else if (old.timeStart > timeStart || old.timeEnd < timeEnd) {
                return new SourceStatisticsRecord(
                        Math.min(timeStart, old.timeStart),
                        Math.max(timeEnd, old.timeEnd),
                        false);
            } else {
                return old;
            }
        }

        SourceStatisticsRecord sentRecord() {
            return new SourceStatisticsRecord(timeStart, timeEnd, true);
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
}
