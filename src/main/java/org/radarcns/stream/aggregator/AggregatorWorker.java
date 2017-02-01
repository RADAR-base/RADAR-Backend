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
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.radarcns.config.KafkaProperty;
import org.radarcns.topic.AvroTopic;
import org.radarcns.util.Monitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runnable abstraction of Kafka Stream Handler
 */
public class AggregatorWorker<K extends SpecificRecord, V extends SpecificRecord,
        T extends AvroTopic<K, V>> implements Thread.UncaughtExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(AggregatorWorker.class);

    private final int numThreads;
    private final String clientId;
    private final MasterAggregator master;
    private final T topic;

    private KafkaStreams streams;
    private KafkaProperty kafkaProperty;

    private Monitor monitor;
    private Timer timer;

    public AggregatorWorker(@Nonnull T topic, @Nonnull String clientId, int numThreads,
            @Nonnull MasterAggregator aggregator, KafkaProperty kafkaProperty) {
        if (numThreads < 1) {
            throw new IllegalStateException(
                    "The number of concurrent threads must be at least 1");
        }
        this.clientId = clientId;
        this.master = aggregator;
        this.topic = topic;
        this.numThreads = numThreads;
        this.kafkaProperty = kafkaProperty;
        this.streams = null;
    }

    /** Create a Kafka Stream builder */
    protected KStreamBuilder getBuilder() throws IOException {
        return new KStreamBuilder();
    }

    /** Create a Monitor to check the stream behaviour*/
    protected void setMonitor(Logger log) {
        this.monitor = new Monitor(log, "records have been read from " + topic.getInputTopic());
        this.timer = new Timer();
    }

    /**
     * It starts the stream and notify the MasterAggregator
     */
    public void start() {
        if (streams != null) {
            throw new IllegalStateException("Cannot start already started stream again");
        }
        log.info("Creating the stream {} from topic {} to topic {}",
                getClientId(), getTopic().getInputTopic(), getTopic().getOutputTopic());

        try {
            streams = new KafkaStreams(getBuilder(),
                    kafkaProperty
                            .getStream(getClientId(), numThreads, DeviceTimestampExtractor.class));
            streams.setUncaughtExceptionHandler(this);
            streams.start();

            if (timer != null) {
                timer.schedule(monitor, 0, 30_000);
            }

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
    }

    protected KafkaStreams getStreams() {
        return streams;
    }

    protected T getTopic() {
        return topic;
    }

    protected void setKafkaProperty(KafkaProperty kafkaProperty) {
        this.kafkaProperty = kafkaProperty;
    }

    protected void incrementMonitor() {
        monitor.increment();
    }
}
