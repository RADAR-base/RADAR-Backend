package org.radarcns.empaticaE4.group;

import org.radarcns.empaticaE4.streams.E4Temperature;
import org.radarcns.stream.aggregator.AggregatorRadar;
import org.radarcns.stream.aggregator.GroupAggregator;

import java.io.IOException;
import java.security.InvalidParameterException;

/**
 * Created by Francesco Nobilia on 21/11/2016.
 */
public class E4TemperatureGroup extends GroupAggregator {

    public E4TemperatureGroup(int numThreads, String clientID) throws InvalidParameterException,IOException {
        super(numThreads,clientID);
    }

    @Override
    public AggregatorRadar createAggregator() throws IOException{
        return new E4Temperature(getPoolName());
    }

}
