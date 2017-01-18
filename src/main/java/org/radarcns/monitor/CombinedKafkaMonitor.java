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

package org.radarcns.monitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs multiple monitors, each in its own thread.
 */
public class CombinedKafkaMonitor implements KafkaMonitor {
    private static final Logger logger = LoggerFactory.getLogger(CombinedKafkaMonitor.class);

    private final List<KafkaMonitor> monitors;
    private final AtomicBoolean done;

    private ExecutorService executor;
    private IOException ioException;
    private InterruptedException interruptedException;

    public CombinedKafkaMonitor(Collection<KafkaMonitor> monitors) {
        if (monitors == null || monitors.isEmpty()) {
            throw new IllegalArgumentException("Monitor collection may not be empty");
        }
        this.monitors = new ArrayList<>(monitors);
        this.done = new AtomicBoolean(false);
        this.executor = null;
        this.ioException = null;
        this.interruptedException = null;
    }

    @Override
    public long getPollTimeout() {
        return monitors.get(0).getPollTimeout();
    }

    @Override
    public void setPollTimeout(long pollTimeout) {
        for (KafkaMonitor monitor : monitors) {
            monitor.setPollTimeout(pollTimeout);
        }
    }

    @Override
    public void start() throws IOException, InterruptedException {
        synchronized (this) {
            if (executor != null) {
                throw new IllegalStateException("Cannot start monitor twice");
            }
            executor = Executors.newFixedThreadPool(monitors.size());
        }

        for (KafkaMonitor monitor : monitors) {
            executor.submit(() -> {
                try {
                    monitor.start();
                } catch (IOException ex) {
                    setIoException(ex);
                } catch (InterruptedException ex) {
                    setInterruptedException(ex);
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(366_000, TimeUnit.DAYS); // > 1000 years...

        if (getIoException() != null) {
            throw getIoException();
        }
        if (getInterruptedException() != null) {
            throw getInterruptedException();
        }
    }

    private synchronized IOException getIoException() {
        return ioException;
    }

    private synchronized void setIoException(IOException ioException) {
        this.ioException = ioException;
        initiateShutdownIgnoreException();
    }

    private synchronized InterruptedException getInterruptedException() {
        return interruptedException;
    }

    private synchronized void setInterruptedException(InterruptedException interruptedException) {
        this.interruptedException = interruptedException;
        initiateShutdownIgnoreException();
    }

    private void initiateShutdownIgnoreException() {
        try {
            initiateShutdown();
        } catch (InterruptedException ex) {
            logger.info("Ignoring additional InterruptedException", ex);
        } catch (IOException ex) {
            logger.info("Ignoring additional IOException", ex);
        }
    }

    private void initiateShutdown() throws IOException, InterruptedException {
        if (!done.getAndSet(true)) {
            for (KafkaMonitor monitor : monitors) {
                monitor.shutdown();
            }
        }
    }

    @Override
    public void shutdown() throws IOException, InterruptedException {
        synchronized (this) {
            if (executor == null) {
                return;
            }
        }
        initiateShutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
        executor.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return done.get();
    }
}
