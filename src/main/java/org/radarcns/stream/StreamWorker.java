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

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.radarcns.config.KafkaProperty;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.kafka.AggregateKey;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.stream.aggregator.DoubleAggregation;
import org.radarcns.stream.aggregator.DoubleArrayAggregation;
import org.radarcns.stream.collector.DoubleArrayCollector;
import org.radarcns.stream.collector.DoubleValueCollector;
import org.radarcns.util.Monitor;
import org.radarcns.util.RadarSingletonFactory;
import org.radarcns.util.RadarUtilities;
import org.radarcns.util.serde.RadarSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.KeyValue.pair;
import static org.radarcns.util.Serialization.floatToDouble;
import static org.radarcns.util.StreamUtil.first;
import static org.radarcns.util.StreamUtil.second;

/**
 * Abstraction of a Kafka Stream.
 */
public abstract class StreamWorker<K extends SpecificRecord, V extends SpecificRecord>
        implements Thread.UncaughtExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(StreamWorker.class);

    private final Logger monitorLog;
    private final int numThreads;
    private final StreamMaster master;
    private final Collection<StreamDefinition> streamDefinitions;
    private final String buildVersion;
    private Collection<ScheduledFuture<?>> monitors;
    private final KafkaProperty kafkaProperty;

    protected final RadarUtilities utilities = RadarSingletonFactory.getRadarUtilities();

    private List<KafkaStreams> streams;

    public StreamWorker(@Nonnull Collection<StreamDefinition> streamDefinitions,
            int numThreads, @Nonnull StreamMaster aggregator, RadarPropertyHandler properties,
            Logger monitorLog) {
        if (numThreads < 1) {
            throw new IllegalStateException(
                    "The number of concurrent threads must be at least 1");
        }
        this.master = aggregator;
        this.streamDefinitions = streamDefinitions;
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

        KStreamBuilder builder = new KStreamBuilder();

        implementStream(def,
                builder.<K, V>stream(def.getInputTopic().getName())
                    .map((k, v) -> {
                        if (monitor != null) {
                            monitor.increment();
                        }
                        return pair(k, v);
                    })
        ).to(def.getOutputTopic().getName());

        return pair(future, new KafkaStreams(builder, getStreamProperties(def)));
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

        return kafkaProperty.getStreamProperties(localClientId, numThreads,
                DeviceTimestampExtractor.class);
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
                .parallelStream()
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

    protected Collection<StreamDefinition> getStreamDefinitions() {
        return streamDefinitions;
    }

    protected final KStream<AggregateKey, DoubleAggregation> aggregateFloat(
            StreamDefinition definition,
            @Nonnull KStream<ObservationKey, V> kstream, Function<V, Float> field) {
        return aggregateDouble(definition, kstream, v -> floatToDouble(field.apply(v)));
    }

    protected final KStream<AggregateKey, DoubleAggregation> aggregateDouble(
            StreamDefinition definition,
            @Nonnull KStream<ObservationKey, V> kstream, Function<V, Double> field) {
        return kstream.groupByKey()
                .aggregate(
                        DoubleValueCollector::new,
                        (k, v, valueCollector) -> valueCollector.add(field.apply(v)),
                        definition.getTimeWindows(),
                        RadarSerdes.getInstance().getDoubleCollector(),
                        definition.getStateStoreName())
                .toStream()
                .map(utilities::collectorToAvro);
    }

    protected final KStream<AggregateKey, DoubleArrayAggregation> aggregateDoubleArray(
            StreamDefinition definition,
            @Nonnull KStream<ObservationKey, V> kstream, Function<V, double[]> field) {
        return kstream.groupByKey()
                .aggregate(
                        DoubleArrayCollector::new,
                        (k, v, valueCollector) -> valueCollector.add(field.apply(v)),
                        definition.getTimeWindows(),
                        RadarSerdes.getInstance().getDoubleArrayCollector(),
                        definition.getStateStoreName())
                .toStream()
                .map(utilities::collectorToAvro);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
