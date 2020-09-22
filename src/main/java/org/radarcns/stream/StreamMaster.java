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
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import org.radarbase.topic.KafkaTopic;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.SingleStreamConfig;
import org.radarcns.config.SubCommand;
import org.radarcns.util.Monitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages a set of {@link StreamWorker} objects.
 */
public class StreamMaster implements SubCommand, UncaughtExceptionHandler {
    private static final Logger logger = LoggerFactory.getLogger(StreamMaster.class);

    public static final int RETRY_TIMEOUT = 300_000; // 5 minutes

    private final List<StreamWorker> streamWorkers;
    private final AtomicInteger currentStream;

    private ScheduledExecutorService executor;

    /**
     * A stream master for given sensor type.
     */
    protected StreamMaster(@Nonnull RadarPropertyHandler propertyHandler,
            @Nonnull Stream<? extends SingleStreamConfig> streams) {
        currentStream = new AtomicInteger(0);
        streamWorkers = streams
                .map(c -> createWorker(propertyHandler, c))
                .collect(Collectors.toList());
        logger.info("Configured streams: \n{}", streamWorkers.stream()
                .map(s -> " - " + s.getClass().getName())
                .collect(Collectors.joining("\n")));
    }

    private StreamWorker createWorker(RadarPropertyHandler config, SingleStreamConfig c) {
        try {
            StreamWorker worker = (StreamWorker) c.getStreamClass().getConstructor().newInstance();
            worker.configure(this, config, c);
            return worker;
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(
                    "Cannot instantiate class " + c.getStreamClass(), e);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException(
                    "Given class " + c.getStreamClass() + " does not implement StreamWorker.", e);
        }
    }

    /** Starts all workers. */
    @Override
    public void start() throws IOException {
        executor = Executors.newSingleThreadScheduledExecutor();
        executor.execute(() -> Thread.currentThread().setUncaughtExceptionHandler(this));

        announceTopics();

        logger.info("Starting all streams");

        List<Exception> exs = streamWorkers.stream()
                .map(worker -> executor.submit(worker::start))
                .map(future -> {
                    try {
                        future.get();
                        return null;
                    } catch (InterruptedException | ExecutionException e) {
                        return e;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        if (!exs.isEmpty()) {
            for (Exception ex : exs) {
                logger.error("Failed to start stream", ex);
            }
            throw new IOException("Failed to start streams", exs.get(0));
        }
    }

    /**
     * Signal all workers to shut down. This does not wait for the workers to shut down.
     */
    @Override
    public void shutdown() throws InterruptedException {
        if (executor.isShutdown()) {
            logger.warn("Streams already shut down, will not shut down again.");
            return;
        }
        logger.info("Shutting down all streams");

        streamWorkers.forEach(worker -> executor.execute(worker::shutdown));
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
    }

    /**
     * Notification from a worker that it has started.
     *
     * @param stream the worker that has started
     */
    public void notifyStartedStream(@Nonnull StreamWorker stream) {
        int current = currentStream.incrementAndGet();
        logger.info("{} is started. {}/{} streams are now running",
                stream, current, streamWorkers.size());
    }

    /**
     * Notification from a worker that it has closed.
     *
     * @param stream the worker that has closed
     */
    public void notifyClosedStream(@Nonnull StreamWorker stream) {
        int current = currentStream.decrementAndGet();

        if (current == 0) {
            logger.info("{} is closed. All streams have been terminated", stream);
        } else {
            logger.info("{} is closed. {}/{} streams are still running",
                    stream, current, streamWorkers.size());
        }
    }

    /**
     * Function used by StreamWorker to notify a crash and trigger a forced shutdown.
     *
     * @param stream the name of the stream that is crashed. Useful for debug purpose
     */
    public void notifyCrashedStream(@Nonnull String stream) {
        logger.error("{} is crashed, forcing shutdown", stream);

        try {
            shutdown();
        } catch (InterruptedException ex) {
            logger.warn("Shutdown interrupted");
        }
    }

    public void restartStream(final StreamWorker worker) {
        logger.info("Restarting stream {}", worker);

        try {
            executor.schedule(worker::start, RETRY_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ex) {
            logger.info("Failed to schedule");
        }
    }

    /**
     * Log the topic list that the application is going to use.
     */
    protected void announceTopics() {
        logger.info("If AUTO.CREATE.TOPICS.ENABLE is FALSE you must create the following topics "
                + "before starting: \n  - {}",
                streamWorkers.stream()
                        .flatMap(StreamWorker::getStreamDefinitions)
                        .flatMap(d -> Stream.of(d.getInputTopic(), d.getOutputTopic()))
                        .filter(Objects::nonNull)
                        .map(KafkaTopic::getName)
                        .sorted()
                        .distinct()
                        .collect(Collectors.joining("\n - ")));
    }

    /** Add a monitor to the master. It will run every 30 seconds. */
    ScheduledFuture<?> addMonitor(Monitor monitor) {
        return executor.scheduleAtFixedRate(monitor, 0, 30, TimeUnit.SECONDS);
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        logger.error("StreamMaster error in Thread {}", t.getName(), e);
        try {
            shutdown();
        } catch (InterruptedException e1) {
            // ignore
        }
    }
}
