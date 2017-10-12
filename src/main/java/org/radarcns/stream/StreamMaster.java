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

import org.radarcns.config.ConfigRadar;
import org.radarcns.config.SubCommand;
import org.radarcns.util.Monitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.radarcns.config.RadarPropertyHandler.Priority.HIGH;
import static org.radarcns.config.RadarPropertyHandler.Priority.LOW;
import static org.radarcns.config.RadarPropertyHandler.Priority.NORMAL;

/**
 * Manages a set of {@link StreamWorker} objects.
 */
public abstract class StreamMaster implements SubCommand {
    private static final Logger log = LoggerFactory.getLogger(StreamMaster.class);

    public static final int RETRY_TIMEOUT = 300_000; // 5 minutes

    private final List<StreamWorker<?, ?>> streamWorkers;
    private final AtomicInteger currentStream;
    private final String nameSensor;
    private int lowPriorityThreads;
    private int normalPriorityThreads;
    private int highPriorityThreads;

    private ScheduledExecutorService executor;

    /**
     * A stream master for given sensor type.
     */
    protected StreamMaster() {
        this.currentStream = new AtomicInteger(0);

        lowPriorityThreads = 1;
        normalPriorityThreads = 1;
        highPriorityThreads = 1;

        streamWorkers = new ArrayList<>();
        nameSensor = getClass().getSimpleName();

        log.info("Creating StreamMaster instance for {}", nameSensor);
    }

    /**
     * Set the number of threads to use for different priorities.
     * @param config configuration for threads
     */
    public synchronized void setNumberOfThreads(ConfigRadar config) {
        if (config.isStandalone()) {
            log.info("[{}] STANDALONE MODE", nameSensor);
            lowPriorityThreads = 1;
            normalPriorityThreads = 1;
            highPriorityThreads = 1;
        } else {
            log.info("[{}] GROUP MODE: {}", nameSensor, config.infoThread());
            lowPriorityThreads = config.threadsByPriority(LOW, 1);
            normalPriorityThreads = config.threadsByPriority(NORMAL, 2);
            highPriorityThreads = config.threadsByPriority(HIGH, 4);
        }
    }

    /**
     * Populates a list with workers.
     *
     * @param list list to add workers to
     */
    protected abstract void createWorkers(List<StreamWorker<?, ?>> list, StreamMaster master);

    /** Starts all workers. */
    @Override
    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor();

        announceTopics();

        log.info("Starting all streams for {}", nameSensor);

        createWorkers(streamWorkers, this);
        streamWorkers.forEach(v -> executor.submit(v::start));
    }

    /**
     * Signal all workers to shut down. This does not wait for the workers to shut down.
     */
    @Override
    public void shutdown() throws InterruptedException {
        log.info("Shutting down all streams for {}", nameSensor);

        while (!streamWorkers.isEmpty()) {
            final StreamWorker<?, ?> worker = streamWorkers.remove(streamWorkers.size() - 1);
            executor.submit(worker::shutdown);
        }

        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
    }

    /**
     * Notification from a worker that it has started.
     *
     * @param stream the worker that has started
     */
    public void notifyStartedStream(@Nonnull StreamWorker<?, ?> stream) {
        int current = currentStream.incrementAndGet();
        log.info("[{}] {} is started. {}/{} streams are now running",
                nameSensor, stream, current, streamWorkers.size());
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
    public void notifyCrashedStream(@Nonnull String stream) throws InterruptedException {
        log.error("[{}] {} is crashed", nameSensor, stream);

        log.info("Forcing shutdown of {}", nameSensor);

        try {
            shutdown();
        } catch (InterruptedException ex) {
            log.warn("Shutdown interrupted");
        }
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
    ScheduledFuture<?> addMonitor(Monitor monitor) {
        return executor.scheduleAtFixedRate(monitor, 0, 30, TimeUnit.SECONDS);
    }

    protected synchronized int lowPriority() {
        return lowPriorityThreads;
    }

    protected synchronized int normalPriority() {
        return normalPriorityThreads;
    }

    protected synchronized int highPriority() {
        return highPriorityThreads;
    }
}
