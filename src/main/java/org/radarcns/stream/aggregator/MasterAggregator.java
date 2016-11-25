package org.radarcns.stream.aggregator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Francesco Nobilia on 24/11/2016.
 */
public abstract class MasterAggregator {

    private final static Logger log = LoggerFactory.getLogger(MasterAggregator.class);

    private static List<AggregatorWorker> list;
    private final String nameSensor;
    private final AtomicInteger currentStream;

    protected MasterAggregator(int numThread, String nameSensor) throws IOException{
        if(numThread < 1){
            throw new IllegalStateException("The number of concurrent threads must be bigger than 0");
        }

        this.nameSensor = nameSensor;
        this.currentStream = new AtomicInteger(0);

        if(numThread == 1){
            log.info("[{}] STANDALONE MODE",nameSensor);
        }
        else{
            log.info("[{}] GROUP MODE: the number of threads per stream is {}",nameSensor,numThread);
        }

        announceTopics(log);

        list = new LinkedList<>();

        createWorker(list, numThread);

        log.info("Creating MasterAggregator instance for {}",nameSensor);
    }

    protected abstract void createWorker(List<AggregatorWorker> list, int numThread) throws IOException;

    protected abstract void announceTopics(Logger log);

    public void start(){
        log.info("Starting all streams for {}",nameSensor);

        list.forEach( v -> v.getThread().start());
    }

    public void shutdown() throws InterruptedException {
        log.info("Shutting down all streams for {}",nameSensor);

        while(!list.isEmpty()){
            list.remove(0).shutdown();
        }
    }

    public void notifyStartedStream(String stream){
        int current = currentStream.incrementAndGet();
        log.info("[{}] {} is started. {}/{} streams are now running",nameSensor,stream,current,list.size());
    }

    public void notifyClosedStream(String stream){
        int current = currentStream.decrementAndGet();
        log.info("[{}] {} is closed. {} streams are now running",nameSensor,stream,current);
    }
}
