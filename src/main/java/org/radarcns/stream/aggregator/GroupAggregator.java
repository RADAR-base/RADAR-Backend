package org.radarcns.stream.aggregator;

import org.apache.kafka.common.errors.IllegalGenerationException;
import org.radarcns.util.RadarThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Created by Francesco Nobilia on 06/10/2016.
 */
public abstract class GroupAggregator implements Runnable,AggregatorWorker{

    private final static Logger log = LoggerFactory.getLogger(GroupAggregator.class);

    private boolean workersCreated = false;

    private final Integer numThreads;
    private ExecutorService executor;
    private List<AggregatorRadar> workers;

    private String poolName;

    public GroupAggregator(int numThreads, String poolName) throws InvalidParameterException,IOException{
        if(numThreads < 1){
            throw new InvalidParameterException("A group must contain at least 2 elements");
        }

        if (poolName == null || poolName.isEmpty()){
            throw new InvalidParameterException("A poolName must be specified");
        }

        this.numThreads = numThreads;
        this.poolName = poolName;

        initiWorkers();
    }

    private void initiWorkers() throws IOException{
        log.trace("initiWorkers");
        List<AggregatorRadar> workerList = getWorkerList();
        synchronized (this) {
            workers = workerList;
            workersCreated = true;
        }
    }

    private List<AggregatorRadar> getWorkerList() throws IOException{
        log.trace("getWorkerList");
        List<AggregatorRadar> list = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            list.add(createAggregator());
        }
        return list;
    }

    public abstract AggregatorRadar createAggregator() throws IOException;

    @Override
    public void run() {
        log.trace("run");

        if (!workersCreated){
            throw new IllegalGenerationException("Before starting the group, initWorkers has to " +
                    "be invoked. For example at the end of the derived class's constructor!");
        }

        ThreadFactory threadFactory = new RadarThreadFactoryBuilder()
                .setNamePrefix(poolName)
                .setDaemon(false)
                .setPriority(Thread.NORM_PRIORITY)
                .build();

        synchronized (this) {
            executor = Executors.newFixedThreadPool(numThreads, threadFactory);
            for (AggregatorRadar worker : workers) {
                executor.submit(worker);
            }
        }
    }

    public synchronized void shutdown() throws InterruptedException {
        log.trace("shutdown");

        if (executor != null){
            executor.shutdown();
        }

        if (workers != null){
            for(AggregatorRadar consumer : workers){
                consumer.shutdown();
            }
        }

        if (executor != null) {
            try {
                if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                    log.info("Timed out waiting for consumer threads to shut down, exiting " +
                            "uncleanly");
                }
            } catch (InterruptedException e) {
                log.error("Interrupted during shutdown, exiting uncleanly");
            }
        }
    }

    public String getPoolName(){
        return this.poolName;
    }

    @Override
    public Thread getThread(){
        Thread thread;

        thread = new Thread(this);
        thread.setName(this.poolName);

        return thread;
    }
}
