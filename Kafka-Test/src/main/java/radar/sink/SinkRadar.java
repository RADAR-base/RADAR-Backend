package radar.sink;

import java.security.InvalidParameterException;

import radar.consumer.ConsumerGroupRadar;

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
