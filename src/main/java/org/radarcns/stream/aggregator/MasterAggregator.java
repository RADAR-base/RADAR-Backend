package org.radarcns.stream.aggregator;

import org.radarcns.config.PropertiesRadar;
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

    protected MasterAggregator(boolean standalone, String nameSensor) throws IOException{
        if(!standalone){
            checkThreadParams();
        }

        this.nameSensor = nameSensor;
        this.currentStream = new AtomicInteger(0);

        int lowPriority = 1;
        int normalPriority = 1;
        int highPriority = 1;

        if(standalone){
            log.info("[{}] STANDALONE MODE",nameSensor);
        }
        else{
            log.info("[{}] GROUP MODE: {}",nameSensor,PropertiesRadar.getInstance().infoThread());

            lowPriority = PropertiesRadar.getInstance().threadsByPriority(PropertiesRadar.Priority.LOW);
            normalPriority = PropertiesRadar.getInstance().threadsByPriority(PropertiesRadar.Priority.NORMAL);
            highPriority = PropertiesRadar.getInstance().threadsByPriority(PropertiesRadar.Priority.HIGH);
        }

        announceTopics(log);

        list = new LinkedList<>();

        createWorker(list, lowPriority, normalPriority, highPriority);

        log.info("Creating MasterAggregator instance for {}",nameSensor);
    }

    protected abstract void createWorker(List<AggregatorWorker> list, int low, int normal, int high) throws IOException;

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

        if(current == 0){
            log.info("[{}] {} is closed. All streams have been terminated",nameSensor,stream);
        }
        else{
            log.info("[{}] {} is closed. {} streams are still running",nameSensor,stream,current);
        }
    }

    private void checkThreadParams(){
        if(PropertiesRadar.getInstance().threadsByPriority(PropertiesRadar.Priority.HIGH) < 1) {
            log.error("Invalid parameter: {} priority threads are {}",
                    PropertiesRadar.Priority.HIGH,
                    PropertiesRadar.getInstance().threadsByPriority(PropertiesRadar.Priority.HIGH));
            throw new IllegalStateException("The number of high priority threads must be an integer bigger than 0");
        }
        if(PropertiesRadar.getInstance().threadsByPriority(PropertiesRadar.Priority.NORMAL) < 1) {
            log.error("Invalid parameter: {} priority threads are {}",
                    PropertiesRadar.Priority.NORMAL,
                    PropertiesRadar.getInstance().threadsByPriority(PropertiesRadar.Priority.NORMAL));
            throw new IllegalStateException("The number of normal priority threads must be an integer bigger than 0");
        }
        if(PropertiesRadar.getInstance().threadsByPriority(PropertiesRadar.Priority.LOW) < 1) {
            log.error("Invalid parameter: {} priority threads are {}",
                    PropertiesRadar.Priority.LOW,
                    PropertiesRadar.getInstance().threadsByPriority(PropertiesRadar.Priority.LOW));
            throw new IllegalStateException("The number of low priority threads must be an integer bigger than 0");
        }
    }
}
