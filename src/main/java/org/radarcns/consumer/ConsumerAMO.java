package org.radarcns.consumer;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.radarcns.util.RadarConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by Francesco Nobilia on 29/09/2016.
 * Abstract class defining the behaviour of a Kafka Consumer that reads message At-Most-Once (AMO)
 * implementing a self commit policy
 */
public abstract class ConsumerAMO<K,V> extends ConsumerRadar{
    private final static Logger log = LoggerFactory.getLogger(ConsumerAMO.class);

    public ConsumerAMO(String clientID, RadarConfig.TopicGroup topics, Properties properties) {
        super(clientID,topics,properties);
    }

    /**
     * Must be implemented for customising consumer behaviour
     * @param record Kafka message currently consumed
     */
    public abstract void process(ConsumerRecord<K,V> record);

    protected void pollMessages() {
        ConsumerRecords<K,V> records = consumer.poll(Long.MAX_VALUE);
        if(doCommitSync()) {
            for (ConsumerRecord record : records) {
                process(record);
            }
        }
    }

    /**
     * Commit the current partition state
     */
    private boolean doCommitSync() {
        try {
            consumer.commitSync();
            return true;
        } catch (CommitFailedException e) {
            log.error("Commit failed", e);
            return false;
        }
    }

    public void shutdown() throws InterruptedException {
        super.shutdown();
        log.info("SHUTDOWN");
    }
}

