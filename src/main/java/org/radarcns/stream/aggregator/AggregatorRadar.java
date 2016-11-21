package org.radarcns.stream.aggregator;

/**
 * Created by Francesco Nobilia on 18/11/2016.
 */
public interface AggregatorRadar extends Runnable,AggregatorWorker{
    String getClientID();
}
