package org.radarcns.consumer;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.radarcns.util.RadarConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by Francesco Nobilia on 29/09/2016.
 * Abstract class defining the behaviour of a Kafka Consumer that reads message At-Least-Once (ALO)
 * implementing a self commit policy
 */
public abstract class ConsumerALO<K,V> extends ConsumerRadar<K, V> {

    private final static Logger log = LoggerFactory.getLogger(ConsumerALO.class);

    public ConsumerALO(String clientID, RadarConfig.TopicGroup topics, Properties properties) {
        super(clientID,topics,properties);
    }

    /**
     * Must be implemented for customising consumer behaviour
     * @param record Kafka message currently consumed
     */
    public abstract void process(ConsumerRecord<K,V> record);

    protected void pollMessages() {
        ConsumerRecords<K,V> records = consumer.poll(Long.MAX_VALUE);
        for (ConsumerRecord<K, V> record : records) {
            process(record);
        }
        doCommitSync();
    }

    /**
     * Commit the current partition state
     */
    private void doCommitSync() {
        try {
            consumer.commitSync();
        } catch (WakeupException e) {
            doCommitSync();
            throw e;
        } catch (CommitFailedException e) {
            log.error("Commit failed", e);
        }
    }

    /**
     * Shutdown the consumer and force the commit.
     * Derived classes must override and call through to the super class's implementation of this
     * method.
     */
    public void shutdown() throws InterruptedException {
        super.shutdown();
        log.info("SHUTDOWN");
    }
}
