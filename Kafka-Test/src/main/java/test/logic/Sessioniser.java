package test.logic;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import java.util.Arrays;

import javax.annotation.Nonnull;

import JavaSessionize.avro.LogLine;
import kafka.message.MessageAndMetadata;
import radar.avro.User;
import test.producer.SimpleProducer;
import test.state.SessionState;
import radar.utils.RadarConfig;
import radar.utils.RadarUtils;

/**
 * Implement the auto for tracking web site users' sessions
 * Created by Francesco Nobilia on 29/09/2016.
 */
public class Sessioniser {

    private final static Logger log = Logger.getLogger(Sessioniser.class);

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
        if(record == null){
            throw new NullPointerException("No input has been provided");
        }

        execute(record,this.producer);
    }

    /**
     * @param record Kafka message currently consumed
     * @throws NullPointerException no input
     */
    public void execute(@Nonnull MessageAndMetadata<Object, Object> record) {
        if (record == null) {
            throw new NullPointerException("No input has been provided");
        }

        execute(record, this.producer);
    }

    /**
     * @param record Kafka message currently consumed
     * @param inputProducer to send back into KafKa the result, if null
     * @throws NullPointerException no input
     */
    public void execute(@Nonnull ConsumerRecord<Object,Object> record, SimpleProducer inputProducer){

        if(record == null){
            throw new NullPointerException("No record has been provided");
        }

        if(inputProducer == null){
            throw new NullPointerException("No producer has been provided");
        }

        log.debug("Involved schemas are: "+ Arrays.toString(RadarUtils.getSchemaName(record)));

        log.debug("Topic:"+record.topic()+" - Partition: "+record.partition()+" Offset: "+record.offset());

        User user = (User) record.key();

        LogLine event = (LogLine) record.value();

        log.debug("Key: "+user.toString()+" - Value: "+event.toString());

        execute(event,user,inputProducer);
    }

    /**
     * @param record Kafka message currently consumed
     * @param inputProducer to send back into KafKa the result, if null
     * @throws NullPointerException no input
     */
    public void execute(@Nonnull MessageAndMetadata<Object, Object> record, SimpleProducer inputProducer){

        if(record == null){
            throw new NullPointerException("No input has been provided");
        }

        log.debug("Topic:"+record.topic()+" - Partition: "+record.partition()+" Offset: "+record.offset());

        GenericRecord genericKey = (GenericRecord) record.key();
        User user = (User) SpecificData.get().deepCopy(User.SCHEMA$, genericKey);

        GenericRecord genericEvent = (GenericRecord) record.message();
        LogLine event = (LogLine) SpecificData.get().deepCopy(LogLine.SCHEMA$, genericEvent);

        log.debug("Key: "+user.toString()+" - Value: "+event.toString());

        if(inputProducer == null){
            inputProducer = this.producer;
        }

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

        if (event == null) {
            throw new NullPointerException("Event is null");
        }

        if (user == null) {
            throw new NullPointerException("User is null");
        }

        RadarConfig config = new RadarConfig();

        SessionState oldState = state.get(user.getIp());
        int sessionId = 0;
        if (oldState == null) {
            state.put(user.getIp().toString(), new SessionState(event.getTimestamp(), 0));
        } else {
            sessionId = oldState.getSessionId();

            if (oldState.getLastConnection() < event.getTimestamp() - config.getSessionTimeWindow()) {
                sessionId = sessionId + 1;
            }

            SessionState newState = new SessionState(event.getTimestamp(), sessionId);
            state.put(user.getIp().toString(), newState);
        }
        event.setSessionid(sessionId);

        inputProducer.send(config.getTopic(RadarConfig.PlatformTopics.out), user, event);
    }

}
