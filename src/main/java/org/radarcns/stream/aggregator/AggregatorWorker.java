package org.radarcns.stream.aggregator;

/**
 * Runnable abstraction of Kafka Stream Handler
 */
public interface AggregatorWorker extends Runnable, Thread.UncaughtExceptionHandler {
    Thread getThread();

    String getName();

    String getClientID();

    void shutdown() throws InterruptedException;
}
