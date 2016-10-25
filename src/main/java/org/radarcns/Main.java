package org.radarcns;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.radarcns.sink.mongoDB.MongoDBSinkRadar;
import org.radarcns.stream.ValueCollector;
import org.radarcns.test.logic.Sessioniser;
import org.radarcns.test.logic.synch.SessioniserALO;
import org.radarcns.test.logic.synch.SessioniserAMO;
import org.radarcns.test.logic.synch.SessioniserGroupALO;
import org.radarcns.test.logic.synch.SessioniserGroupAMO;
import org.radarcns.test.producer.SimpleProducer;
import org.radarcns.test.stream.Statistics;
import org.radarcns.util.RadarConfig;

import radarcns.KeyRadar;
import radarcns.ValueRadar;

/**
 * Created by francesco on 05/09/16.
 */

public class Main {

    private final static Logger log = Logger.getLogger(Main.class);

    //Test case
    private enum TestCase {
        ALO, AMO, GROUP_AMO, GROUP_ALO
    }
    final static int sequence = 10;
    final static long sleep = 10000;
    private static TestCase test = TestCase.ALO;

    private final static AtomicBoolean shutdown = new AtomicBoolean(false);

    private static SessioniserAMO sessioniserAMO;

    private static SessioniserALO sessioniserALO;

    private static SessioniserGroupALO sessioniserGroupALO;

    private static SessioniserGroupAMO sessioniserGroupAMO;

    private static Sessioniser sessioniser;
    private static int numThread = 3;

    private static MongoDBSinkRadar mongoDBSink;

    private static Statistics statistics;

    private static Thread producerThread;
    private static Thread consumerThread;
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

        /*consumerThread = getConsumer();
        consumerThread.start();*/

        streamThread = getStream();
        streamThread.start();

        /*connectorThread = getConnector();
        connectorThread.start();*/
    }

    private static void finish() throws InterruptedException{
        shutdown.set(true);

        /*switch (test){
            case ALO:
                sessioniserALO.shutdown();
                break;
            case AMO:
                sessioniserAMO.shutdown();
                break;
            case GROUP_ALO:
                sessioniserGroupALO.shutdown();
                break;
            case GROUP_AMO:
                sessioniserGroupAMO.shutdown();
                break;
            default:break;
        }*/

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

                KeyRadar key = new KeyRadar("user","device");
                ValueCollector collector = new ValueCollector();

                while(!shutdown.get()) {

                    for (; offset < sequence; offset++) {

                        java.util.Date date= new java.util.Date();
                        String timestamp = new Timestamp(date.getTime()).toString();
                        ValueRadar value = new ValueRadar(timestamp,r.nextDouble());

                        empatica_e4_inter_beat_interval heart_interval = new em

                        collector.add(value);

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

    private static Thread getConsumer(){
        Thread thread;
        switch (test){
            case ALO:
                sessioniserALO = new SessioniserALO();
                thread = new Thread(sessioniserALO);
                thread.setName("Consumer-ALO");
                return thread;
            case AMO:
                sessioniserAMO = new SessioniserAMO();
                thread = new Thread(sessioniserAMO);
                thread.setName("Consumer-AMO");
                return thread;
            case GROUP_ALO:
                sessioniserGroupALO = new SessioniserGroupALO(numThread);
                thread = new Thread(sessioniserGroupALO);
                thread.setName("ConsumerGroup-ALO");
                return thread;
            case GROUP_AMO:
                sessioniserGroupAMO = new SessioniserGroupAMO(numThread);
                thread = new Thread(sessioniserGroupAMO);
                thread.setName("ConsumerGroup-AMO");
                return thread;
            default:
                return null;
        }
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