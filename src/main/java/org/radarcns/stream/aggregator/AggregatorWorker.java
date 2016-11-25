package org.radarcns.stream.aggregator;

/**
 * Created by Francesco Nobilia on 21/11/2016.
 */
public interface AggregatorWorker extends Runnable {
    Thread getThread();

    String getName();

    String getClientID();

    void shutdown() throws InterruptedException;
}
