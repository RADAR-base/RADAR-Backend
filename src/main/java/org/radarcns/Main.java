package org.radarcns;

import org.apache.log4j.Logger;
import org.radarcns.empaticaE4.EmpaticaE4InterBeatInterval;
import org.radarcns.key.MeasurementKey;
import org.radarcns.sink.mongoDB.MongoDBSinkRadar;
import org.radarcns.stream.ValueCollector;
import org.radarcns.test.producer.SimpleProducer;
import org.radarcns.test.stream.Statistics;
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

    final static int sequence = 10;
    final static long sleep = 8000;

    private final static AtomicBoolean shutdown = new AtomicBoolean(false);

    private static MongoDBSinkRadar mongoDBSink;

    private static Statistics statistics;

    private static Thread producerThread;
    private static Thread connectorThread;
    private static Thread streamThread;

    public static void main(String[] args) throws InterruptedException,IOException {
        go();
        sleep();
        finish();
    }

    private static void go() throws IOException{
        producerThread = getProducer();
        producerThread.start();

        streamThread = getStream();
        streamThread.start();

        connectorThread = getConnector();
        connectorThread.start();
    }

    private static void finish() throws InterruptedException{
        shutdown.set(true);

        if(mongoDBSink != null){
            mongoDBSink.shutdown();
        }

        if(statistics != null){
            statistics.shutdown();
        }
    }

    private static void sleep(){
        try{
            Thread.sleep(sleep);
        }catch(InterruptedException e){
            log.error("Got interrupted in main thread!",e);
        }
    }

    private static Thread getProducer(){
        Thread thread = new Thread(new Runnable() {
            public void run() {

                SimpleProducer producer = new SimpleProducer();
                RadarConfig prop = new RadarConfig();

                Random r = new Random();

                int offset = 0;

                MeasurementKey key = new MeasurementKey("user","device");
                ValueCollector collector = new ValueCollector();

                while(!shutdown.get()) {

                    for (; offset < sequence; offset++) {

                        java.util.Date date= new java.util.Date();

                        double timestamp = new Double(new Long(date.getTime()).toString());
                        timestamp = timestamp / 1000d;

                        EmpaticaE4InterBeatInterval value = new EmpaticaE4InterBeatInterval(timestamp,timestamp,r.nextFloat());

                        collector.add(RadarUtils.ibiToHR(value.getInterBeatInterval()));

                        producer.send("input-statistic",key,value);
                    }
                }

                System.out.println(collector);

                producer.shutdown();
            }

        });

        thread.setName("Producer");

        return thread;
    }

    private static Thread getConnector(){
        Thread thread;

        mongoDBSink = new MongoDBSinkRadar();
        thread = new Thread(mongoDBSink);
        thread.setName("MongoDB-Sink");

        return thread;
    }

    private static Thread getStream() throws IOException{
        Thread thread;

        statistics = new Statistics();
        thread = new Thread(statistics);
        thread.setName("Streaming");

        return thread;
    }
}