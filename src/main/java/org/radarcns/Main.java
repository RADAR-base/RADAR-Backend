package org.radarcns;

import org.apache.log4j.Logger;
import org.radarcns.empaticaE4.E4Worker;
import org.radarcns.empaticaE4.EmpaticaE4InterBeatInterval;
import org.radarcns.empaticaE4.streams.E4InterBeatInterval;
import org.radarcns.key.MeasurementKey;
import org.radarcns.sink.mongoDB.MongoDBSinkRadar;
import org.radarcns.stream.collector.DoubleValueCollector;
import org.radarcns.test.producer.SimpleProducer;
import org.radarcns.util.RadarConfig;
import org.radarcns.util.RadarUtils;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by francesco on 05/09/16.
 */

public class Main {

    private final static Logger log = Logger.getLogger(Main.class);

    final static long sleep = 3600;

    private final static AtomicBoolean shutdown = new AtomicBoolean(false);

    private static MongoDBSinkRadar mongoDBSink;

    private static Thread connectorThread;

    public static void main(String[] args) throws InterruptedException,IOException {
        go();
        sleep();
        finish();
    }

    private static void go() throws IOException{
        /*connectorThread = getConnector();
        connectorThread.start();*/

        E4Worker.getSingletonInstance().start();

    }

    private static void finish() throws InterruptedException, IOException {
        shutdown.set(true);

        if(mongoDBSink != null){
            mongoDBSink.shutdown();
        }

        E4Worker.getSingletonInstance().shutdown();
    }

    private static void sleep(){
        try{
            Thread.sleep(sleep);
        }catch(InterruptedException e){
            log.error("Got interrupted in main thread!",e);
        }
    }

    private static Thread getConnector(){
        Thread thread;

        mongoDBSink = new MongoDBSinkRadar();
        thread = new Thread(mongoDBSink);
        thread.setName("MongoDB-Sink");

        return thread;
    }
}