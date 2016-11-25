package org.radarcns.sink;

import org.radarcns.consumer.ConsumerGroupRadar;

import java.security.InvalidParameterException;

/**
 * Created by Francesco Nobilia on 10/10/2016.
 */
public abstract class SinkRadar extends ConsumerGroupRadar{

    public SinkRadar(int numThreads) throws InvalidParameterException {
        super(numThreads);
    }

    public SinkRadar(int numThreads, String poolName) throws InvalidParameterException {
        super(numThreads, poolName);
    }
}
