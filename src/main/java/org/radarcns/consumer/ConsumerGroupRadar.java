package org.radarcns.consumer;

import org.apache.kafka.common.errors.IllegalGenerationException;
import org.radarcns.util.RadarThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public abstract class ConsumerGroupRadar implements Runnable{

    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupRadar.class);

    private boolean workersCreated = false;

    private final Integer numThreads;
    private ExecutorService executor;
    private List<ConsumerRadar> workers;

    private String poolName;

    public ConsumerGroupRadar(int numThreads, String poolName) throws InvalidParameterException{
        this(numThreads);

        if (poolName != null && !poolName.isEmpty()){
            this.poolName = poolName+"-Consumer";
        }
    }

    public ConsumerGroupRadar(int numThreads) throws InvalidParameterException{
        if(numThreads < 1){
            throw new InvalidParameterException("A group must contain at least 2 elements");
        }

        this.numThreads = numThreads;
        this.poolName = "GroupPool-Consumer";
    }

    public void initiWorkers(){
        log.trace("initiWorkers");
        List<ConsumerRadar> workerList = getWorkerList();
        synchronized (this) {
            workers = workerList;
            workersCreated = true;
        }
    }

    private List<ConsumerRadar> getWorkerList(){
        log.trace("getWorkerList");
        List<ConsumerRadar> list = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            list.add(createConsumer());
        }
        return list;
    }

    public abstract ConsumerRadar createConsumer();

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
            for (ConsumerRadar worker : workers) {
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
            for(ConsumerRadar consumer : workers){
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
}
