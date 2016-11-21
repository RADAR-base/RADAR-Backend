package org.radarcns.empaticaE4;

import org.radarcns.empaticaE4.group.E4BatteryLevelGroup;
import org.radarcns.empaticaE4.group.E4BloodVolumePulseGroup;
import org.radarcns.empaticaE4.group.E4ElectroDermalActivityGroup;
import org.radarcns.empaticaE4.group.E4HeartRateGroup;
import org.radarcns.empaticaE4.group.E4InterBeatIntervalGroup;
import org.radarcns.empaticaE4.group.E4TemperatureGroup;
import org.radarcns.empaticaE4.streams.E4BatteryLevel;
import org.radarcns.empaticaE4.streams.E4BloodVolumePulse;
import org.radarcns.empaticaE4.streams.E4ElectroDermalActivity;
import org.radarcns.empaticaE4.streams.E4HeartRate;
import org.radarcns.empaticaE4.streams.E4InterBeatInterval;
import org.radarcns.empaticaE4.streams.E4Temperature;
import org.radarcns.stream.aggregator.AggregatorWorker;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Francesco Nobilia on 21/11/2016.
 */
public class E4Worker{

    private static List<AggregatorWorker> list;

    private final static Object syncObject = new Object();
    private static E4Worker instance = null;

    private final boolean group;

    public static E4Worker getSingletonInstance() throws IOException{
        synchronized (syncObject) {
            if (instance == null) {
                instance = new E4Worker(false, 0);
            }

            if(instance.isGroup()){
                throw new IllegalStateException("You cannot create a singleton instance while a group one is running");
            }

            return instance;
        }
    }

    public static E4Worker getGroupInstance(int numThread) throws IOException{
        synchronized (syncObject) {
            if (instance == null) {
                instance = new E4Worker(true, numThread);
            }

            if(!instance.isGroup()){
                throw new IllegalStateException("You cannot create a group instance while a singleton one is running");
            }

            return instance;
        }
    }

    private E4Worker(boolean group,int numThread) throws IOException{

        this.group = group;

        list = new LinkedList<>();

        if(group){
            list.add(new E4BatteryLevelGroup(numThread,"E4BatteryLevel"));
            list.add(new E4BloodVolumePulseGroup(numThread,"E4BloodVolumePulse"));
            list.add(new E4ElectroDermalActivityGroup(numThread,"E4ElectroDermalActivity"));
            list.add(new E4HeartRateGroup(numThread,"E4HeartRate"));
            list.add(new E4InterBeatIntervalGroup(numThread,"E4InterBeatInterval"));
            list.add(new E4TemperatureGroup(numThread,"E4Temperature"));
        }
        else{
            list.add(new E4BatteryLevel("E4BatteryLevel"));
            list.add(new E4BloodVolumePulse("E4BloodVolumePulse"));
            list.add(new E4ElectroDermalActivity("E4ElectroDermalActivity"));
            list.add(new E4HeartRate("E4HeartRate"));
            list.add(new E4InterBeatInterval("E4InterBeatInterval"));
            list.add(new E4Temperature("E4Temperature"));
        }
    }

    public void start(){
        list.forEach( v -> v.getThread().start());
    }

    public void shutdown() throws InterruptedException {
        for (AggregatorWorker aw : list){
            aw.shutdown();
        }


    }

    public boolean isGroup(){
        return this.group;
    }
}
