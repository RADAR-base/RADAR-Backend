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

package org.radarcns.stream;

import static org.apache.kafka.streams.KeyValue.pair;
import static org.radarcns.util.StreamUtil.first;
import static org.radarcns.util.StreamUtil.second;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.radarcns.config.KafkaProperty;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.kafka.AggregateKey;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.stream.aggregator.AggregateList;
import org.radarcns.stream.aggregator.NumericAggregate;
import org.radarcns.stream.collector.AggregateListCollector;
import org.radarcns.stream.collector.NumericAggregateCollector;
import org.radarcns.util.Monitor;
import org.radarcns.util.RadarSingletonFactory;
import org.radarcns.util.RadarUtilities;
import org.radarcns.util.serde.RadarSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstraction of a Kafka Stream.
 */
public abstract class KStreamWorker<K extends SpecificRecord, V extends SpecificRecord>
        extends AbstractStreamWorker implements Thread.UncaughtExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(KStreamWorker.class);

    private final Logger monitorLog;
    private final int numThreads;
    private final StreamMaster master;
    private final String buildVersion;
    private Collection<ScheduledFuture<?>> monitors;
    private final KafkaProperty kafkaProperty;

    protected final RadarUtilities utilities = RadarSingletonFactory.getRadarUtilities();

    private List<KafkaStreams> streams;

    public KStreamWorker(int numThreads, @Nonnull StreamMaster master,
            RadarPropertyHandler properties, Logger monitorLog) {
        if (numThreads < 1) {
            throw new IllegalStateException(
                    "The number of concurrent threads must be at least 1");
        }
        this.master = master;
        this.numThreads = numThreads;
        this.buildVersion = properties.getRadarProperties().getBuildVersion();
        this.kafkaProperty = properties.getKafkaProperties();
        this.streams = null;
        this.monitors = null;
        this.monitorLog = monitorLog;
    }

    /**
     * Create a Kafka Stream builder. This implementation will create a stream from given
     * input topic to given output topic. It monitors the amount of messages that are read.
     */
    protected KeyValue<ScheduledFuture<?>, KafkaStreams> createBuilder(StreamDefinition def) {
        Monitor monitor;
        ScheduledFuture<?> future = null;
        if (monitorLog != null) {
            monitor = new Monitor(monitorLog, "records have been read from "
                    + def.getInputTopic() + " to " + def.getOutputTopic());
            future = master.addMonitor(monitor);
        } else {
            monitor = null;
        }

        StreamsBuilder builder = new StreamsBuilder();

        implementStream(def,
                builder.<K, V>stream(def.getInputTopic().getName())
                        .map((k, v) -> {
                            if (monitor != null) {
                                monitor.increment();
                            }
                            return pair(k, v);
                        })
        ).to(def.getOutputTopic().getName());

        return pair(future, new KafkaStreams(builder.build(), getStreamProperties(def)));
    }

    /**
     * @return Properties for a Kafka Stream
     */
    protected Properties getStreamProperties(@Nonnull StreamDefinition definition) {
        String localClientId = getClass().getName() + "-" + buildVersion;
        TimeWindows window = definition.getTimeWindows();
        if (window != null) {
            localClientId += '-' + window.sizeMs + '-' + window.advanceMs;
        }

        Properties props = kafkaProperty.getStreamProperties(localClientId, numThreads,
                DeviceTimestampExtractor.class);
        long interval = (long)(ThreadLocalRandom.current().nextDouble(0.75, 1.25)
                * definition.getCommitInterval().toMillis());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
                String.valueOf(interval));

        return props;
    }

    /**
     * Defines the stream computation.
     */
    protected abstract KStream<?, ?> implementStream(StreamDefinition definition,
            @Nonnull KStream<K, V> kstream);

    /**
     * Starts the stream and notify the StreamMaster.
     */
    public void start() {
        if (streams != null) {
            throw new IllegalStateException("Streams already started. Cannot start them again.");
        }

        List<KeyValue<ScheduledFuture<?>, KafkaStreams>> streamBuilders = getStreamDefinitions()
                .map(this::createBuilder)
                .collect(Collectors.toList());

        monitors = streamBuilders.stream()
                .map(first())
                .collect(Collectors.toList());

        streams = streamBuilders.stream()
                .map(second())
                .collect(Collectors.toList());

        streams.forEach(stream -> {
            stream.setUncaughtExceptionHandler(this);
            stream.start();
        });

        master.notifyStartedStream(this);
    }

    /**
     * Close the stream and notify the StreamMaster.
     */
    public void shutdown() {
        log.info("Shutting down {} stream", getClass().getSimpleName());

        closeStreams();

        master.notifyClosedStream(this);
    }

    /**
     * Handles exceptions that have been uncaught. It is called when a StreamThread is
     * terminating due to an exception.
     */
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        log.error("Thread {} has been terminated due to {}", t.getName(), e.getMessage(), e);

        closeStreams();

        if (e instanceof StreamsException) {
            master.restartStream(this);
        } else {
            master.notifyCrashedStream(getClass().getSimpleName());
        }
    }

    private void closeStreams() {
        if (streams != null) {
            streams.forEach(KafkaStreams::close);
            streams = null;
        }

        if (monitors != null) {
            monitors.forEach(f -> f.cancel(false));
            monitors = null;
        }
    }

    protected final KStream<AggregateKey, NumericAggregate> aggregateNumeric(
            @Nonnull StreamDefinition definition, @Nonnull KStream<ObservationKey, V> kstream,
            @Nonnull String fieldName, @Nonnull Schema schema) {
        return kstream.groupByKey()
                .windowedBy(definition.getTimeWindows())
                .aggregate(
                        () -> new NumericAggregateCollector(fieldName, schema),
                        (k, v, valueCollector) -> valueCollector.add(v),
                        RadarSerdes.materialized(definition.getStateStoreName(),
                            RadarSerdes.getInstance().getNumericAggregateCollector()))
                .toStream()
                .map(utilities::numericCollectorToAvro);
    }

    protected final KStream<AggregateKey, NumericAggregate> aggregateCustomNumeric(
            @Nonnull StreamDefinition definition, @Nonnull KStream<ObservationKey, V> kstream,
            @Nonnull Function<V, Double> calculation, @Nonnull String fieldName) {
        return kstream.groupByKey()
                .windowedBy(definition.getTimeWindows())
                .aggregate(
                        () -> new NumericAggregateCollector(fieldName),
                        (k, v, valueCollector) -> valueCollector.add(calculation.apply(v)),
                        RadarSerdes.materialized(definition.getStateStoreName(),
                            RadarSerdes.getInstance().getNumericAggregateCollector()))
                .toStream()
                .map(utilities::numericCollectorToAvro);
    }

    protected final KStream<AggregateKey, AggregateList> aggregateFields(
            @Nonnull StreamDefinition definition, @Nonnull KStream<ObservationKey, V> kstream,
            @Nonnull String[] fieldNames, @Nonnull Schema schema) {

        return kstream.groupByKey()
                .windowedBy(definition.getTimeWindows())
                .aggregate(
                        () -> new AggregateListCollector(fieldNames, schema),
                        (k, v, valueCollector) -> valueCollector.add(v),
                        RadarSerdes.materialized(definition.getStateStoreName(),
                            RadarSerdes.getInstance().getAggregateListCollector()))
                .toStream()
                .map(utilities::listCollectorToAvro);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
