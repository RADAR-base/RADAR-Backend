package org.radarcns.stream.aggregator;

import org.radarcns.config.PropertiesRadar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

/**
 * Abstraction of a set of AggregatorWorker
 * @see org.radarcns.stream.aggregator.AggregatorWorker
 */
public abstract class MasterAggregator {

    private final static Logger log = LoggerFactory.getLogger(MasterAggregator.class);

    private static List<AggregatorWorker> list;
    private final String nameSensor;
    private final AtomicInteger currentStream;

    /**
     * @param standalone: true means that the aggregator will assign one thread per stream
     * @param nameSensor: the name of the device that produced data that will be consumed. Only for debug
     */
    protected MasterAggregator(boolean standalone,@Nonnull String nameSensor) throws IOException{
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

    /**
     * Function called by the constructor to populate the AggregatorWorker list
     * @param list:
     * @param low,normal,high: are the three available priority levels that can be used to start kafka streams
     */
    protected abstract void createWorker(@Nonnull List<AggregatorWorker> list, int low, int normal, int high) throws IOException;

    /**
     * Informative function to log the topics list that the application is going to use
     * @param log: the logger instance that will be used to notify the user
     */
    protected abstract void announceTopics(@Nonnull Logger log);

    /**
     * It starts all AggregatorWorkers controlled by this MasterAggregator
     */
    public void start(){
        log.info("Starting all streams for {}",nameSensor);

        list.forEach( v -> v.getThread().start());
    }

    /**
     * It stops all AggregatorWorkers controlled by this MasterAggregator
     */
    public void shutdown() throws InterruptedException {
        log.info("Shutting down all streams for {}",nameSensor);

        while(!list.isEmpty()){
            list.remove(0).shutdown();
        }
    }

    /**
     * Informative function used by AggregatorWorker to notify that it has started its managed stream
     * @param stream: the name of the stream that has been started. Useful for debug purpose
     */
    public void notifyStartedStream(@Nonnull String stream){
        int current = currentStream.incrementAndGet();
        log.info("[{}] {} is started. {}/{} streams are now running",nameSensor,stream,current,list.size());
    }

    /**
     * Informative function used by AggregatorWorker to notify that it has closed its managed stream
     * @param stream: the name of the stream that has been closed. Useful for debug purpose
     */
    public void notifyClosedStream(@Nonnull String stream){
        int current = currentStream.decrementAndGet();

        if(current == 0){
            log.info("[{}] {} is closed. All streams have been terminated",nameSensor,stream);
        }
        else{
            log.info("[{}] {} is closed. {} streams are still running",nameSensor,stream,current);
        }
    }

    /**
     * It checks if the priority params specified by the user can be used or not.
     * TODO: this check can be moved in the org.radarcns.config.PropertiesRadar class.
     * TODO: A valuable enhancement is checking whether the involved topics have as many partitions as the number of starting threads
     */
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
