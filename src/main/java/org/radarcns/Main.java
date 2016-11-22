package org.radarcns;

import org.apache.log4j.Logger;
import org.radarcns.empaticaE4.E4Worker;
import org.radarcns.empaticaE4.streams.E4BloodVolumePulse;
import org.radarcns.sink.mongoDB.MongoDBSinkRadar;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by francesco on 05/09/16.
 */

public class Main {

    private final static Logger log = Logger.getLogger(Main.class);

    final static long sleep = 3600*2;

    private final static AtomicBoolean shutdown = new AtomicBoolean(false);

    private static MongoDBSinkRadar mongoDBSink;

    private static Thread connectorThread;

    private static Thread tsThread;
    private static E4BloodVolumePulse ebvp;

    public static void main(String[] args) throws InterruptedException,IOException {
        log.trace("Init");
        go();
        sleep();
        finish();
        log.trace("Finish");

        /*Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                streams.close();
            }
        }));*/
    }

    private static void go() throws IOException{
        log.trace("Init");
        E4Worker.getInstance().start();
        //E4Worker.getGroupInstance(2).start();
        log.trace("Finish");
    }

    private static void finish() throws InterruptedException, IOException {
        log.trace("Init");

        if(mongoDBSink != null){
            mongoDBSink.shutdown();
        }

        E4Worker.getInstance().shutdown();

        log.trace("Finish");
    }

    private static void sleep(){
        log.trace("Init");
        try{
            Thread.sleep(sleep);
        }catch(InterruptedException e){
            log.error("Got interrupted in main thread!",e);
        }
        log.trace("Finish");
    }

    private static Thread getConnector(){
        Thread thread;

        mongoDBSink = new MongoDBSinkRadar();
        thread = new Thread(mongoDBSink);
        thread.setName("MongoDB-Sink");

        return thread;
    }
}