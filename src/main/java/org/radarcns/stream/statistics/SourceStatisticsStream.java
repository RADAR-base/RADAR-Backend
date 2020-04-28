package org.radarcns.stream.statistics;

import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.radarcns.monitor.AbstractKafkaMonitor.extractKey;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.radarcns.config.SourceStatisticsStreamConfig;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.stream.AbstractStreamWorker;
import org.radarcns.stream.SourceStatistics;
import org.radarcns.stream.StreamDefinition;
import org.radarcns.util.serde.RadarSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceStatisticsStream extends AbstractStreamWorker {
    private static final Logger logger = LoggerFactory.getLogger(SourceStatisticsStream.class);
    private String name;
    private Duration interval;

    @Override
    protected List<KafkaStreams> createStreams() {
        return Collections.singletonList(new KafkaStreams(getTopology(), getStreamsConfig()));
    }

    @Override
    protected void doCleanup() {
        // do nothing
    }

    @Override
    protected void initialize() {
        SourceStatisticsStreamConfig config = (SourceStatisticsStreamConfig) this.config;

        this.name = config.getName();

        List<String> inputTopics = config.getTopics();
        if (inputTopics == null || inputTopics.isEmpty()) {
            throw new IllegalArgumentException("Input topics for stream " + name + " is empty");
        }
        if (config.getOutputTopic() == null) {
            throw new IllegalArgumentException("Output topic for stream " + name + " is missing");
        }
        inputTopics.forEach(t -> defineStream(t, config.getOutputTopic()));

        this.interval = Duration.ofMillis(config.getFlushTimeout());
    }

    protected Duration getInterval() {
        return interval;
    }

    private Topology getTopology() {
        Topology topology = new Topology();

        final Map<String, ?> serdeConfig = Map.of(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                getStreamsConfig().get(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG),
                AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS,
                true);

        addSource("source", topology, serdeConfig);

        topology.addProcessor("process", SourceStatisticsProcessor::new, "source");

        addSink(topology, serdeConfig, "process");
        addStateStore(topology, serdeConfig, "process");

        return topology;
    }

    private void addSource(String name, Topology topology, Map<String, ?> serdeConfig) {
        GenericAvroDeserializer genericReaderKey = new GenericAvroDeserializer();
        GenericAvroDeserializer genericReaderValue = new GenericAvroDeserializer();

        genericReaderKey.configure(serdeConfig, true);
        genericReaderValue.configure(serdeConfig, false);

        String[] inputTopics = getStreamDefinitions()
                .map(s -> s.getInputTopic().getName())
                .toArray(String[]::new);

        topology.addSource(name, genericReaderKey, genericReaderValue, inputTopics);
    }

    private void addStateStore(
            Topology topology,
            Map<String, ?> serdeConfig,
            String... processorNames) {
        Serde<ObservationKey> keySerde = new SpecificAvroSerde<>();
        Serde<SourceStatisticsRecord> valueSerde =
                new RadarSerde<>(SourceStatisticsRecord.class).getSerde();

        keySerde.configure(serdeConfig, true);
        valueSerde.configure(serdeConfig, false);

        StoreBuilder<KeyValueStore<ObservationKey, SourceStatisticsRecord>> statisticsStore =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("statistics"),
                        keySerde,
                        valueSerde);

        topology.addStateStore(statisticsStore, processorNames);
    }

    private void addSink(Topology topology, Map<String, ?> serdeConfig, String... parentNames) {
        String outputTopic = getStreamDefinitions()
                .map(StreamDefinition::getOutputTopic)
                .filter(Objects::nonNull)
                .findAny()
                .orElseThrow(() -> new IllegalStateException(
                        "Output topic for SourceStatisticsStream " + name
                                + " is undefined."))
                .getName();

        SpecificAvroSerializer<ObservationKey> keySerializer =
                new SpecificAvroSerializer<>();
        SpecificAvroSerializer<SourceStatistics> valueSerializer =
                new SpecificAvroSerializer<>();

        keySerializer.configure(serdeConfig, true);
        valueSerializer.configure(serdeConfig, false);

        topology.addSink("sink",
                outputTopic,
                keySerializer,
                valueSerializer,
                parentNames);
    }

    private Properties getStreamsConfig() {
        Properties settings = kafkaProperty.getStreamProperties(name, config);
        settings.remove(DEFAULT_KEY_SERDE_CLASS_CONFIG);
        settings.remove(DEFAULT_VALUE_SERDE_CLASS_CONFIG);
        return settings;
    }

    private class SourceStatisticsProcessor implements Processor<GenericRecord, GenericRecord> {
        private KeyValueStore<ObservationKey, SourceStatisticsRecord> store;
        private ProcessorContext context;
        private Cancellable punctuateCancellor;
        private Duration localInterval = Duration.ZERO;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            store = (KeyValueStore<ObservationKey, SourceStatisticsRecord>) context
                    .getStateStore("statistics");
            this.context = context;
            updatePunctuate();
        }

        private void updatePunctuate() {
            if (!localInterval.equals(getInterval())) {
                localInterval = getInterval();
                if (punctuateCancellor != null) {
                    punctuateCancellor.cancel();
                }
                punctuateCancellor = this.context.schedule(
                        localInterval, PunctuationType.WALL_CLOCK_TIME, this::sendNew);
            }
        }

        @SuppressWarnings({"unused", "PMD.AccessorMethodGeneration"})
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

            store.putAll(sent);
            context.commit();

            updatePunctuate();
        }

        @SuppressWarnings("PMD.AccessorMethodGeneration")
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
            SourceStatisticsRecord newStats = SourceStatisticsRecord
                    .updateRecord(stats, timeStart, timeEnd);
            if (!newStats.equals(stats)) {
                store.put(key, newStats);
            }
        }

        @Override
        public void close() {
            // do nothing
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class SourceStatisticsRecord {
        private final double timeStart;
        private final double timeEnd;
        private final boolean isSent;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
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
