package org.radarcns.stream.aggregator;

/**
 * Created by Francesco Nobilia on 21/11/2016.
 */
public interface AggregatorWorker {
    Thread getThread();

    void shutdown() throws InterruptedException;
}
