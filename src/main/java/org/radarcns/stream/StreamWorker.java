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

import java.io.IOException;
import javax.annotation.Nonnull;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.radarcns.config.KafkaProperty;
import org.radarcns.util.Monitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstraction of a Kafka Stream.
 */
public abstract class StreamWorker<K extends SpecificRecord, V extends SpecificRecord>
        implements Thread.UncaughtExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(StreamWorker.class);

    private final int numThreads;
    private final String clientId;
    private final StreamMaster master;
    private final StreamDefinition streamDefinition;
    private final Monitor monitor;
    private final KafkaProperty kafkaProperty;

    private KafkaStreams streams;

    public StreamWorker(@Nonnull StreamDefinition streamDefinition, @Nonnull String clientId,
            int numThreads, @Nonnull StreamMaster aggregator, KafkaProperty kafkaProperty,
            Logger monitorLog) {
        if (numThreads < 1) {
            throw new IllegalStateException(
                    "The number of concurrent threads must be at least 1");
        }
        this.clientId = clientId;
        this.master = aggregator;
        this.streamDefinition = streamDefinition;
        this.numThreads = numThreads;
        this.kafkaProperty = kafkaProperty;
        this.streams = null;

        if (monitorLog == null) {
            this.monitor = null;
        } else {
            this.monitor = new Monitor(monitorLog, "records have been read from "
                    + streamDefinition.getInputTopic());
        }
    }

    /**
     * Create a Kafka Stream builder. This implementation will create a stream from given
     * input topic to given output topic. It monitors the amount of messages that are read.
     */
    protected KStreamBuilder createBuilder() throws IOException {
        KStreamBuilder builder = new KStreamBuilder();

        StreamDefinition definition = getStreamDefinition();
        String inputTopic = definition.getInputTopic().getName();
        String outputTopic = definition.getOutputTopic().getName();

        KStream<K, V> inputStream = builder.<K, V>stream(inputTopic)
                .map((k, v) -> {
                    incrementMonitor();
                    return new KeyValue<>(k, v);
                });

        defineStream(inputStream).to(outputTopic);

        return builder;
    }

    /**
     * Defines the stream computation.
     */
    protected abstract KStream<?, ?> defineStream(@Nonnull KStream<K, V> kstream);

    /**
     * Starts the stream and notify the StreamMaster.
     */
    public void start() {
        if (streams != null) {
            throw new IllegalStateException("Streams already started. Cannot start them again.");
        }

        log.info("Creating the stream {} from topic {} to topic {}",
                clientId, streamDefinition.getInputTopic(),
                streamDefinition.getOutputTopic());

        try {
            if (monitor != null) {
                master.addMonitor(monitor);
            }
            streams = new KafkaStreams(createBuilder(),
                    kafkaProperty.getStream(clientId, numThreads, DeviceTimestampExtractor.class));
            streams.setUncaughtExceptionHandler(this);
            streams.start();

            master.notifyStartedStream(this);
        } catch (IOException ex) {
            uncaughtException(Thread.currentThread(), ex);
        }
    }

    /**
     * Close the stream and notify the StreamMaster.
     */
    public void shutdown() {
        log.info("Shutting down {} stream", clientId);

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
            master.notifyCrashedStream(clientId);
        }
    }

    private void closeStreams() {
        if (streams != null) {
            streams.close();
            streams = null;
        }
    }

    protected StreamDefinition getStreamDefinition() {
        return streamDefinition;
    }

    /** Increment the number of messages processed. */
    protected void incrementMonitor() {
        monitor.increment();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + '<' + clientId + '>';
    }
}
