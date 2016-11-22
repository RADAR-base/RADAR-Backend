package org.radarcns.empaticaE4;

import org.apache.log4j.Logger;
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

    private final static Logger log = Logger.getLogger(E4Worker.class);

    private static List<AggregatorWorker> list;

    private final static Object syncObject = new Object();
    private static E4Worker instance = null;

    public static E4Worker getInstance() throws IOException{
        log.trace("Init");
        synchronized (syncObject) {
            if (instance == null) {
                instance = new E4Worker(1);
            }
        }

        log.trace(instance.toString());

        log.trace("Finish");
        return instance;
    }

    public static E4Worker getInstance(int numThread) throws IOException{
        log.trace("Init");
        synchronized (syncObject) {
            if (instance == null) {
                instance = new E4Worker(numThread);
            }
        }
        log.trace("Finish");

        return instance;
    }

    private E4Worker(int numThread) throws IOException{
        log.trace("Init");

        if(numThread < 1){
            throw new IllegalStateException("The number of concurrent threads must be bigger than 0");
        }

        list = new LinkedList<>();

        list.add(new E4BatteryLevel("E4BatteryLevel",numThread));
        list.add(new E4BloodVolumePulse("E4BloodVolumePulse_KCL",numThread));
        list.add(new E4ElectroDermalActivity("E4ElectroDermalActivity",numThread));
        list.add(new E4HeartRate("E4HeartRate",numThread));
        list.add(new E4InterBeatInterval("E4InterBeatInterval",numThread));
        list.add(new E4Temperature("E4Temperature",numThread));

        log.trace("Finish");
    }

    public void start(){
        log.trace("Init");
        //list.forEach( v -> v.getThread().start());
        for (AggregatorWorker aw : list){

            log.trace(aw.toString());

            Thread thread;

            thread = new Thread(aw);
            thread.setName(aw.getName());

            thread.start();
        }
        log.trace("Finish");
    }

    public void shutdown() throws InterruptedException {
        log.trace("Init");
        for (AggregatorWorker aw : list){
            log.trace(aw.toString());
            aw.shutdown();
        }
        log.trace("Finish");
    }
}
