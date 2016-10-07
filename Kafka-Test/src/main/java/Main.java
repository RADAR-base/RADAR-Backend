import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import JavaSessionize.avro.LogLine;
import radar.avro.User;
import radar.consumer.commit.auto.StreamConsumer;
import radar.consumer.commit.auto.StreamConsumerGroup;
import test.producer.SimpleProducer;
import test.logic.Sessioniser;
import test.logic.auto.SessioniserGroupWorker;
import test.logic.auto.SessioniserStreamed;
import test.logic.synch.SessioniserALO;
import test.logic.synch.SessioniserAMO;
import test.logic.synch.SessioniserGroupALO;
import test.logic.synch.SessioniserGroupAMO;
import radar.utils.RadarConfig;

/**
 * Created by francesco on 05/09/16.
 */

public class Main {

    private final static Logger log = Logger.getLogger(Main.class);

    //Test case
    private enum TestCase {
        STREAMED, ALO, AMO, GROUP_STREAM, GROUP_AMO, GROUP_ALO
    }
    final static int sequence = 10;
    final static long sleep = 60000;
    private static TestCase test = TestCase.GROUP_ALO;

    private final static AtomicBoolean shutdown = new AtomicBoolean(false);

    private static SessioniserStreamed sessioniserStreamed;

    private static SessioniserAMO sessioniserAMO;

    private static SessioniserALO sessioniserALO;

    private static SessioniserGroupALO sessioniserGroupALO;

    private static SessioniserGroupAMO sessioniserGroupAMO;

    private static StreamConsumerGroup sessioniserStreamedGroup;
    private static Sessioniser sessioniser;
    private static int numThread = 3;

    private static Thread producerThread;
    private static Thread consumerThread;

    public static void main(String[] args) throws InterruptedException {
        go();
        sleep();
        finish();
    }

    private static void go(){
        producerThread = getProducer();
        producerThread.start();

        consumerThread = getConsumer();
        consumerThread.start();
    }

    private static void finish() throws InterruptedException{
        shutdown.set(true);

        switch (test){
            case STREAMED:
                sessioniserStreamed.shutdown();
                break;
            case ALO:
                sessioniserALO.shutdown();
                break;
            case AMO:
                sessioniserAMO.shutdown();
                break;
            case GROUP_STREAM:
                sessioniserStreamedGroup.shutdown();
                sessioniser.shutdown();
                break;
            case GROUP_ALO:
                sessioniserGroupALO.shutdown();
                break;
            case GROUP_AMO:
                sessioniserGroupAMO.shutdown();
                break;
            default:break;
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

                while(!shutdown.get()) {

                    for (; offset < sequence; offset++) {
                        Object[] temp = producer.testResourceSchema();

                        User key = (User) temp[0];
                        LogLine value = (LogLine) temp[1];

                        producer.send(prop.getTopic(RadarConfig.PlatformTopics.in),key,value);

                        try {
                            int upperBound = Integer.valueOf(prop.getSessionTimeWindow().toString()).intValue();
                            upperBound += 1000;
                            long sleepInterval = (long) r.nextInt(upperBound);
                            Thread.sleep(sleepInterval);
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
            case STREAMED:
                sessioniserStreamed = new SessioniserStreamed();
                thread = new Thread(sessioniserStreamed);
                thread.setName("Consumer-Streamed");
                return thread;
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
            case GROUP_STREAM:
                sessioniser = new Sessioniser();
                List<StreamConsumer> workers = new LinkedList<>();
                for(int i=0; i<numThread; i++){
                    workers.add(i,new SessioniserGroupWorker(sessioniser));
                }

                sessioniserStreamedGroup = new StreamConsumerGroup(3,workers);
                thread = new Thread(sessioniserStreamedGroup);
                thread.setName("ConsumerGroup-STREAMED");
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
}