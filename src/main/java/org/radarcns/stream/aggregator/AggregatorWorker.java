/*
 * Copyright 2017 Kings College London and The Hyve
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

package org.radarcns.stream.aggregator;

import java.io.IOException;
import java.util.Timer;
import javax.annotation.Nonnull;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.radarcns.config.KafkaProperty;
import org.radarcns.stream.StreamDefinition;
import org.radarcns.util.Monitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runnable abstraction of Kafka Stream Handler
 */
public abstract class AggregatorWorker<K extends SpecificRecord, V extends SpecificRecord>
        implements Thread.UncaughtExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(AggregatorWorker.class);

    private final int numThreads;
    private final String clientId;
    private final MasterAggregator master;
    private final StreamDefinition streamDefinition;
    private final Monitor monitor;

    private KafkaStreams streams;
    private KafkaProperty kafkaProperty;
    private Timer timer;

    public AggregatorWorker(@Nonnull StreamDefinition streamDefinition, @Nonnull String clientId,
            int numThreads, @Nonnull MasterAggregator aggregator, KafkaProperty kafkaProperty,
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

        if (log == null) {
            this.monitor = null;
        } else {
            this.monitor = new Monitor(monitorLog, "records have been read from "
                    + streamDefinition.getInputTopic());
        }
    }

    /** Create a Kafka Stream builder */
    protected KStreamBuilder getBuilder() throws IOException {
        KStreamBuilder builder = new KStreamBuilder();

        String inputTopic = getStreamDefinition().getInputTopic().getName();
        setStream(builder.stream(inputTopic));

        return builder;
    }

    /**
     * @implSpec it defines the stream computation
     */
    protected abstract void setStream(@Nonnull KStream<K, V> kstream) throws IOException;

    /**
     * It starts the stream and notify the MasterAggregator
     */
    public void start() {
        if (streams != null) {
            throw new IllegalStateException("Cannot start already started stream again");
        }
        log.info("Creating the stream {} from topic {} to topic {}",
                getClientId(), getStreamDefinition().getInputTopic(),
                getStreamDefinition().getOutputTopic());

        try {
            if (monitor != null) {
                timer = new Timer();
                timer.schedule(monitor, 0, 30_000);
            }
            streams = new KafkaStreams(getBuilder(),
                    kafkaProperty
                            .getStream(getClientId(), numThreads, DeviceTimestampExtractor.class));
            streams.setUncaughtExceptionHandler(this);
            streams.start();

            master.notifyStartedStream(clientId);
        } catch (IOException ex) {
            uncaughtException(Thread.currentThread(), ex);
        }
    }

    /**
     * It closes the stream and notify the MasterAggregator
     */
    public void shutdown() {
        log.info("Shutting down {} stream", getClientId());

        closeStreams();

        master.notifyClosedStream(clientId);
    }

    public String getClientId() {
        return clientId;
    }

    /**
     * It handles exceptions that have been uncaught. It is called when a StreamThread is
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
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
    }

    protected KafkaStreams getStreams() {
        return streams;
    }

    protected StreamDefinition getStreamDefinition() {
        return streamDefinition;
    }

    protected void setKafkaProperty(KafkaProperty kafkaProperty) {
        this.kafkaProperty = kafkaProperty;
    }

    protected void incrementMonitor() {
        monitor.increment();
    }
}
