package org.radarcns;

import radar.User;
import JavaSessionize.LogLine;
import org.radarcns.sink.mongoDB.MongoDBSinkRadar;
import org.radarcns.test.logic.synch.SessioniserALO;
import org.radarcns.test.logic.synch.SessioniserAMO;
import org.radarcns.test.logic.synch.SessioniserGroupALO;
import org.radarcns.test.logic.synch.SessioniserGroupAMO;
import org.radarcns.test.producer.SimpleProducer;
import org.radarcns.test.stream.ActiveUser;
import org.radarcns.util.RadarConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by francesco on 05/09/16.
 */

public class Main {

    private final static Logger log = LoggerFactory.getLogger(Main.class);

    //Test case
    private enum TestCase {
        ALO, AMO, GROUP_AMO, GROUP_ALO
    }
    final static int sequence = 10;
    final static long sleep = 20000;
    private static TestCase test = TestCase.ALO;

    private final static AtomicBoolean shutdown = new AtomicBoolean(false);

    private static SessioniserAMO sessioniserAMO;

    private static SessioniserALO sessioniserALO;

    private static SessioniserGroupALO sessioniserGroupALO;

    private static SessioniserGroupAMO sessioniserGroupAMO;

    private final static int numThread = 3;

    private static MongoDBSinkRadar mongoDBSink;

    private static ActiveUser activeUser;

    public static void main(String[] args) throws InterruptedException,IOException {
        go();
        sleep();
        finish();
    }

    private static void go() throws IOException{
        Thread producerThread = getProducer();
        producerThread.start();

        Thread consumerThread = getConsumer();
        consumerThread.start();

        Thread streamThread = getStream();
        streamThread.start();

        Thread connectorThread = getConnector();
        connectorThread.start();
    }

    private static void finish() throws InterruptedException{
        shutdown.set(true);

        switch (test){
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
        }

        if(mongoDBSink != null){
            mongoDBSink.shutdown();
        }

        if(activeUser != null){
            activeUser.shutdown();
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

                int sleepUpperBound = prop.getSessionTimeWindow() + 1000;

                while(!shutdown.get()) {

                    for (; offset < sequence; offset++) {
                        Object[] temp = producer.testResourceSchema();

                        User key = (User) temp[0];
                        LogLine value = (LogLine) temp[1];

                        producer.send(prop.getTopic(RadarConfig.TopicGroup.in),key,value);

                        try {
                            Thread.sleep(r.nextInt(sleepUpperBound));
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }

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

        activeUser = new ActiveUser();
        thread = new Thread(activeUser);
        thread.setName("Streaming");

        return thread;
    }
}