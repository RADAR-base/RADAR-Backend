package org.radarcns.empaticaE4;

import org.radarcns.empaticaE4.streams.E4Acceleration;
import org.radarcns.empaticaE4.streams.E4BatteryLevel;
import org.radarcns.empaticaE4.streams.E4BloodVolumePulse;
import org.radarcns.empaticaE4.streams.E4ElectroDermalActivity;
import org.radarcns.empaticaE4.streams.E4HeartRate;
import org.radarcns.empaticaE4.streams.E4InterBeatInterval;
import org.radarcns.empaticaE4.streams.E4Temperature;
import org.radarcns.empaticaE4.topic.E4Topics;
import org.radarcns.stream.aggregator.AggregatorWorker;
import org.radarcns.stream.aggregator.MasterAggregator;
import org.radarcns.util.RadarSingletonFactory;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

/**
 * Singleton MasterAggregator for Empatica E4
 * @see org.radarcns.stream.aggregator.MasterAggregator
 */
public class E4Worker extends MasterAggregator {
    private static final Object syncObject = new Object();
    private static E4Worker instance = null;

    public static E4Worker getInstance() throws IOException{
        synchronized (syncObject) {
            if (instance == null) {
                instance = new E4Worker(RadarSingletonFactory.getRadarPropertyHandler().getRadarProperties().isStandalone());
            }
            return instance;
        }
    }

    private E4Worker(boolean standalone) throws IOException{
        super(standalone,"Empatica E4");
    }

    @Override
    protected void announceTopics(@Nonnull Logger log){
        log.info("If AUTO.CREATE.TOPICS.ENABLE is FALSE you must create the following topics "
                + "before starting: \n  - {}",
                String.join("\n  - ", E4Topics.getInstance().getTopicNames()));
    }

    @Override
    protected void createWorker(@Nonnull List<AggregatorWorker> list, int low, int normal, int high) throws IOException {
        list.add(new E4Acceleration("E4Acceleration",high,this));
        list.add(new E4BatteryLevel("E4BatteryLevel",low,this));
        list.add(new E4BloodVolumePulse("E4BloodVolumePulse",high,this));
        list.add(new E4ElectroDermalActivity("E4ElectroDermalActivity",normal,this));
        list.add(new E4HeartRate("E4HeartRate",high,this));
        list.add(new E4InterBeatInterval("E4InterBeatInterval",high,this));
        list.add(new E4Temperature("E4Temperature",high,this));
    }
}
