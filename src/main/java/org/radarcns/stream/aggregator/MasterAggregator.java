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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import org.radarcns.config.ConfigRadar;
import org.radarcns.config.RadarPropertyHandler.Priority;
import org.radarcns.config.SubCommand;
import org.radarcns.util.RadarSingletonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstraction of a set of AggregatorWorker
 *
 * @see org.radarcns.stream.aggregator.AggregatorWorker
 */
public abstract class MasterAggregator implements SubCommand {
    private static final Logger log = LoggerFactory.getLogger(MasterAggregator.class);

    public static final int RETRY_TIMEOUT = 300_000; // 5 minutes

    private final List<AggregatorWorker<?,?,?>> list;
    private final String nameSensor;
    private final AtomicInteger currentStream;
    private int lowPriority;
    private int normalPriority;
    private int highPriority;

    private ConfigRadar configRadar =
            RadarSingletonFactory.getRadarPropertyHandler().getRadarProperties();
    private final ScheduledExecutorService executor;

    /**
     * @param standalone true means that the aggregator will assign one thread per stream
     * @param nameSensor the name of the device that produced data that will be consumed. Only for
     *                   debug
     */
    protected MasterAggregator(boolean standalone, @Nonnull String nameSensor) throws IOException {
        this.nameSensor = nameSensor;
        this.currentStream = new AtomicInteger(0);

        lowPriority = 1;
        normalPriority = 1;
        highPriority = 1;

        if (standalone) {
            log.info("[{}] STANDALONE MODE", nameSensor);
        } else {
            log.info("[{}] GROUP MODE: {}", nameSensor, this.configRadar.infoThread());
            lowPriority = configRadar.threadsByPriority(Priority.LOW, 1);
            normalPriority = configRadar.threadsByPriority(Priority.NORMAL, 2);
            highPriority = configRadar.threadsByPriority(Priority.HIGH, 4);
        }

        executor = Executors.newSingleThreadScheduledExecutor();

        announceTopics(log);

        list = new ArrayList<>();

        log.info("Creating MasterAggregator instance for {}", nameSensor);
    }

    /**
     * Populates an AggregatorWorker list with workers
     *
     * @param low,normal,high: are the three available priority levels that can be used to start
     *                       kafka streams
     */
    protected abstract List<AggregatorWorker<?,?,?>> createWorkers(int low, int normal, int high);

    /**
     * Informative function to log the topics list that the application is going to use
     *
     * @param log the logger instance that will be used to notify the user
     */
    protected abstract void announceTopics(@Nonnull Logger log);

    /** It starts all AggregatorWorkers controlled by this MasterAggregator */
    public void start() throws IOException {
        log.info("Starting all streams for {}", nameSensor);

        list.addAll(createWorkers(lowPriority, normalPriority, highPriority));
        list.forEach(v -> executor.submit(v::start));
    }

    /** It stops all AggregatorWorkers controlled by this MasterAggregator */
    public void shutdown() throws InterruptedException {
        log.info("Shutting down all streams for {}", nameSensor);

        while (!list.isEmpty()) {
            final AggregatorWorker<?, ?, ?> worker = list.remove(list.size() - 1);
            executor.submit(worker::shutdown);
        }

        executor.shutdown();
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
        log.error("[{}] {} is crashed", nameSensor, stream);

        log.info("Forcing shutdown of {}", nameSensor);

        //TODO implement forcing shutdown
    }

    public void restartStream(final AggregatorWorker<?, ?, ?> worker) {
        log.info("Restarting stream {} for {}", worker.getClientId(), nameSensor);

        try {
            executor.schedule(worker::start, RETRY_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ex) {
            log.info("Failed to schedule");
        }
    }

    protected void setConfigRadar(ConfigRadar configRadar) {
        this.configRadar = configRadar;
    }
}
