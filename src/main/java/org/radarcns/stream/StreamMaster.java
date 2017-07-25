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
import org.radarcns.util.Monitor;
import org.radarcns.util.RadarSingletonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstraction of a set of StreamWorker
 *
 * @see StreamWorker
 */
public abstract class StreamMaster implements SubCommand {
    private static final Logger log = LoggerFactory.getLogger(StreamMaster.class);

    public static final int RETRY_TIMEOUT = 300_000; // 5 minutes

    private final List<StreamWorker<?,?>> list;
    private final String nameSensor;
    private final AtomicInteger currentStream;
    private int lowPriority;
    private int normalPriority;
    private int highPriority;

    private ConfigRadar configRadar =
            RadarSingletonFactory.getRadarPropertyHandler().getRadarProperties();
    private ScheduledExecutorService executor;

    /**
     * @param standalone true means that the aggregator will assign one thread per stream
     * @param nameSensor the name of the device that produced data that will be consumed. Only for
     *                   debug
     */
    protected StreamMaster(boolean standalone, @Nonnull String nameSensor) {
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

        list = new ArrayList<>();

        log.info("Creating StreamMaster instance for {}", nameSensor);
    }

    /**
     * Populates an StreamWorker list with workers
     *
     * @param low,normal,high: are the three available priority levels that can be used to start
     *                       kafka streams
     */
    protected abstract List<StreamWorker<?,?>> createWorkers(int low, int normal, int high);

    /** It starts all AggregatorWorkers controlled by this StreamMaster */
    @Override
    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor();

        announceTopics();

        log.info("Starting all streams for {}", nameSensor);

        list.addAll(createWorkers(lowPriority, normalPriority, highPriority));
        list.forEach(v -> executor.submit(v::start));
    }

    /** It stops all AggregatorWorkers controlled by this StreamMaster */
    @Override
    public void shutdown() throws InterruptedException {
        log.info("Shutting down all streams for {}", nameSensor);

        while (!list.isEmpty()) {
            final StreamWorker<?, ?> worker = list.remove(list.size() - 1);
            executor.submit(worker::shutdown);
        }

        executor.shutdown();
    }

    /**
     * Notification from StreamWorker that it has started its managed stream
     *
     * @param stream the name of the stream that has been started. Useful for debug purpose
     */
    public void notifyStartedStream(@Nonnull String stream) {
        int current = currentStream.incrementAndGet();
        log.info("[{}] {} is started. {}/{} streams are now running",
                nameSensor, stream, current, list.size());
    }

    /**
     * Notification from StreamWorker that it has closed its managed stream
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
     * Function used by StreamWorker to notify a crash and trigger a forced shutdown.
     *
     * @param stream the name of the stream that is crashed. Useful for debug purpose
     */
    public void notifyCrashedStream(@Nonnull String stream) {
        log.error("[{}] {} is crashed", nameSensor, stream);

        log.info("Forcing shutdown of {}", nameSensor);

        //TODO implement forcing shutdown
    }

    public void restartStream(final StreamWorker<?, ?> worker) {
        log.info("Restarting stream {} for {}", worker.getClientId(), nameSensor);

        try {
            executor.schedule(worker::start, RETRY_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ex) {
            log.info("Failed to schedule");
        }
    }

    /**
     * Informative function to log the topics list that the application is going to use
     */
    protected void announceTopics() {
        log.info("If AUTO.CREATE.TOPICS.ENABLE is FALSE you must create the following topics "
                        + "before starting: \n  - {}",
                String.join("\n  - ", getStreamGroup().getTopicNames()));
    }


    protected void setConfigRadar(ConfigRadar configRadar) {
        this.configRadar = configRadar;
    }

    protected abstract StreamGroup getStreamGroup();

    /** Add a monitor to the master. It will run every 30 seconds. */
    void addMonitor(Monitor monitor) {
        executor.scheduleAtFixedRate(monitor, 0, 30, TimeUnit.SECONDS);
    }
}
