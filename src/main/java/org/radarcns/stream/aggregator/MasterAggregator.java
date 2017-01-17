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

import org.radarcns.config.SubCommand;
import org.radarcns.config.ConfigRadar;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.util.RadarSingletonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstraction of a set of AggregatorWorker
 *
 * @see org.radarcns.stream.aggregator.AggregatorWorker
 */
public abstract class MasterAggregator implements SubCommand {
    private static final Logger log = LoggerFactory.getLogger(MasterAggregator.class);

    private final List<AggregatorWorker> list;
    private final String nameSensor;
    private final AtomicInteger currentStream;
    private ConfigRadar configRadar =
            RadarSingletonFactory.getRadarPropertyHandler().getRadarProperties();

    /**
     * @param standalone true means that the aggregator will assign one thread per stream
     * @param nameSensor the name of the device that produced data that will be consumed. Only for
     *                   debug
     */
    protected MasterAggregator(boolean standalone, @Nonnull String nameSensor) throws IOException {
        if (!standalone) {
            checkThreadParams();
        }

        this.nameSensor = nameSensor;
        this.currentStream = new AtomicInteger(0);

        int lowPriority = 1;
        int normalPriority = 1;
        int highPriority = 1;

        if (standalone) {
            log.info("[{}] STANDALONE MODE", nameSensor);
        } else {
            log.info("[{}] GROUP MODE: {}", nameSensor, this.configRadar.infoThread());

            lowPriority = configRadar.threadsByPriority(RadarPropertyHandler.Priority.LOW);
            normalPriority = configRadar.threadsByPriority(RadarPropertyHandler.Priority.NORMAL);
            highPriority = configRadar.threadsByPriority(RadarPropertyHandler.Priority.HIGH);
        }

        announceTopics(log);

        list = new ArrayList<>();

        createWorker(list, lowPriority, normalPriority, highPriority);

        log.info("Creating MasterAggregator instance for {}", nameSensor);
    }

    /**
     * Populates an AggregatorWorker list with workers
     *
     * @param list list to add workers to
     * @param low,normal,high: are the three available priority levels that can be used to start
     *                       kafka streams
     */
    protected abstract void createWorker(
            @Nonnull List<AggregatorWorker> list, int low, int normal, int high) throws IOException;

    /**
     * Informative function to log the topics list that the application is going to use
     *
     * @param log the logger instance that will be used to notify the user
     */
    protected abstract void announceTopics(@Nonnull Logger log);

    /** It starts all AggregatorWorkers controlled by this MasterAggregator */
    public void start() {
        log.info("Starting all streams for {}", nameSensor);

        list.forEach(v -> v.getThread().start());
    }

    /** It stops all AggregatorWorkers controlled by this MasterAggregator */
    public void shutdown() throws InterruptedException {
        log.info("Shutting down all streams for {}", nameSensor);

        while (!list.isEmpty()) {
            list.remove(0).shutdown();
        }
    }

    /**
     * Notification from AggregatorWorker that it has started its managed stream
     *
     * @param stream the name of the stream that has been started. Useful for debug purpose
     */
    public void notifyStartedStream(@Nonnull String stream) {
        int current = currentStream.incrementAndGet();
        log.info("[{}] {} is started. {}/{} streams are now running",
                nameSensor, stream, current, list.size());
    }

    /**
     * Notification from AggregatorWorker that it has closed its managed stream
     *
     * @param stream the name of the stream that has been closed. Useful for debug purpose
     */
    public void notifyClosedStream(@Nonnull String stream) {
        int current = currentStream.decrementAndGet();

        if (current == 0) {
            log.info("[{}] {} is closed. All streams have been terminated", nameSensor, stream);
        } else {
            log.info("[{}] {} is closed. {} streams are still running",
                    nameSensor, stream, current);
        }
    }

    /**
     * Function used by AggregatorWorker to notify a crash and trigger a forced shutdown.
     *
     * @param stream the name of the stream that is crashed. Useful for debug purpose
     */
    public void notifyCrashedStream(@Nonnull String stream) {
        log.warn("[{}] {} is crashed", nameSensor, stream);

        log.info("Forcing shutdown of {}", nameSensor);

        //TODO implement forcing shutdown
    }

    /**
     * It checks if the priority params specified by the user can be used or not.
     * TODO: this check can be moved in the org.radarcns.config.PropertiesRadar class.
     * TODO: A valuable enhancement is checking whether the involved topics have as many partitions
     *       as the number of starting threads
     */
    private void checkThreadParams() {
        if (this.configRadar.threadsByPriority(RadarPropertyHandler.Priority.HIGH) < 1) {
            log.error("Invalid parameter: {} priority threads are {}",
                    RadarPropertyHandler.Priority.HIGH,
                    this.configRadar.threadsByPriority(RadarPropertyHandler.Priority.HIGH));
            throw new IllegalStateException(
                    "The number of high priority threads must be an integer bigger than 0");
        }
        if (this.configRadar.threadsByPriority(RadarPropertyHandler.Priority.NORMAL) < 1) {
            log.error("Invalid parameter: {} priority threads are {}",
                    RadarPropertyHandler.Priority.NORMAL,
                    this.configRadar.threadsByPriority(RadarPropertyHandler.Priority.NORMAL));
            throw new IllegalStateException(
                    "The number of normal priority threads must be an integer bigger than 0");
        }
        if (this.configRadar.threadsByPriority(RadarPropertyHandler.Priority.LOW) < 1) {
            log.error("Invalid parameter: {} priority threads are {}",
                    RadarPropertyHandler.Priority.LOW,
                    this.configRadar.threadsByPriority(RadarPropertyHandler.Priority.LOW));
            throw new IllegalStateException(
                    "The number of low priority threads must be an integer bigger than 0");
        }
    }

    protected void setConfigRadar(ConfigRadar configRadar) {
        this.configRadar = configRadar;
    }
}
