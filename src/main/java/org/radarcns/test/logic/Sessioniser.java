package org.radarcns.test.logic;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import java.util.Arrays;

import javax.annotation.Nonnull;

import radar.User;
import JavaSessionize.LogLine;

import org.radarcns.util.RadarConfig;
import org.radarcns.util.RadarUtils;
import org.radarcns.test.producer.SimpleProducer;
import org.radarcns.test.state.SessionState;
import org.slf4j.LoggerFactory;

/**
 * Implement the auto for tracking web site users' sessions
 * Created by Francesco Nobilia on 29/09/2016.
 */
public class Sessioniser {

    private final static Logger log = LoggerFactory.getLogger(Sessioniser.class);

    private final NonBlockingHashMap<String,SessionState> state;

    private final SimpleProducer producer;

    public Sessioniser(){
        producer = new SimpleProducer();
        state = new NonBlockingHashMap<>();
    }

    public void shutdown(){
        producer.shutdown();
        log.info(state.size()+" analysed users!");
    }

    /**
     * @param record Kafka message currently consumed
     * @throws NullPointerException no input
     */
    public void execute(@Nonnull ConsumerRecord<Object,Object> record){
        execute(record,this.producer);
    }

    /**
     * @param record Kafka message currently consumed
     * @param inputProducer to send back into KafKa the result, if null
     * @throws NullPointerException no input
     */
    public void execute(@Nonnull ConsumerRecord<Object,Object> record, @Nonnull SimpleProducer inputProducer){
        log.debug("Involved schemas are: "+ Arrays.toString(RadarUtils.getSchemaName(record)));

        log.debug("AvroTopic:"+record.topic()+" - Partition: "+record.partition()+" Offset: "+record.offset());

        User user = (User) record.key();

        LogLine event = (LogLine) record.value();

        log.debug("Key: "+user.toString()+" - Value: "+event.toString());

        execute(event,user,inputProducer);
    }

    /**
     * Implement the auto to check if two consecutive connections belong to the same session
     * @param event history of current web navigation
     * @param user
     * @param inputProducer producer to finalise the analysis
     * @throws NullPointerException no input
     */
    public void execute(@Nonnull LogLine event, @Nonnull User user, @Nonnull SimpleProducer inputProducer) {
        RadarConfig config = new RadarConfig();

        SessionState oldState = state.get(user.getIp());
        int sessionId = 0;
        if (oldState == null) {
            state.put(user.getIp(), new SessionState(event.getTimestamp(), 0));
        } else {
            sessionId = oldState.getSessionId();

            if (oldState.getLastConnection() < event.getTimestamp() - config.getSessionTimeWindow()) {
                sessionId = sessionId + 1;
            }

            SessionState newState = new SessionState(event.getTimestamp(), sessionId);
            state.put(user.getIp(), newState);
        }
        event.setSessionid(sessionId);

        inputProducer.send(config.getTopic(RadarConfig.TopicGroup.out), user, event);
    }

}
