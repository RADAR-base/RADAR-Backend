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
 * Manages a set of {@link StreamWorker} objects.
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

    private ScheduledExecutorService executor;

    /**
     * A stream master for given sensor type.
     * @param standalone true means that the aggregator will assign one thread per stream
     * @param nameSensor the name of the device that produced data that will be consumed. Only for
     *                   debugging
     */
    protected StreamMaster(boolean standalone, String nameSensor) {
        this.nameSensor = nameSensor;
        this.currentStream = new AtomicInteger(0);

        lowPriority = 1;
        normalPriority = 1;
        highPriority = 1;

        if (standalone) {
            log.info("[{}] STANDALONE MODE", nameSensor);
        } else {
            ConfigRadar configRadar = RadarSingletonFactory.getRadarPropertyHandler()
                    .getRadarProperties();
            log.info("[{}] GROUP MODE: {}", nameSensor, configRadar.infoThread());
            lowPriority = configRadar.threadsByPriority(Priority.LOW, 1);
            normalPriority = configRadar.threadsByPriority(Priority.NORMAL, 2);
            highPriority = configRadar.threadsByPriority(Priority.HIGH, 4);
        }

        list = new ArrayList<>();

        log.info("Creating StreamMaster instance for {}", nameSensor);
    }

    /**
     * Populates a list with workers.
     *
     * @param list list to add workers to
     * @param low number of threads to use if a stream has low priority
     * @param normal number of threads to use if a stream has normal priority
     * @param high number of threads to use if a stream has high priority
     */
    protected abstract void createWorkers(List<StreamWorker<?, ?>> list,
            int low, int normal, int high);

    /** Starts all workers. */
    @Override
    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor();

        announceTopics();

        log.info("Starting all streams for {}", nameSensor);

        createWorkers(list, lowPriority, normalPriority, highPriority);
        list.forEach(v -> executor.submit(v::start));
    }

    /**
     * Signal all workers to shut down. This does not wait for the workers to shut down.
     */
    @Override
    public void shutdown() {
        log.info("Shutting down all streams for {}", nameSensor);

        while (!list.isEmpty()) {
            final StreamWorker<?, ?> worker = list.remove(list.size() - 1);
            executor.submit(worker::shutdown);
        }

        executor.shutdown();
    }

    /**
     * Notification from a worker that it has started.
     *
     * @param stream the worker that has started
     */
    public void notifyStartedStream(@Nonnull StreamWorker<?, ?> stream) {
        int current = currentStream.incrementAndGet();
        log.info("[{}] {} is started. {}/{} streams are now running",
                nameSensor, stream, current, list.size());
    }

    /**
     * Notification from a worker that it has closed.
     *
     * @param stream the worker that has closed
     */
    public void notifyClosedStream(@Nonnull StreamWorker<?, ?> stream) {
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
        log.info("Restarting stream {} for {}", worker, nameSensor);

        try {
            executor.schedule(worker::start, RETRY_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ex) {
            log.info("Failed to schedule");
        }
    }

    /**
     * Log the topic list that the application is going to use.
     */
    protected void announceTopics() {
        log.info("If AUTO.CREATE.TOPICS.ENABLE is FALSE you must create the following topics "
                        + "before starting: \n  - {}",
                String.join("\n  - ", getStreamGroup().getTopicNames()));
    }

    protected abstract StreamGroup getStreamGroup();

    /** Add a monitor to the master. It will run every 30 seconds. */
    void addMonitor(Monitor monitor) {
        executor.scheduleAtFixedRate(monitor, 0, 30, TimeUnit.SECONDS);
    }
}
