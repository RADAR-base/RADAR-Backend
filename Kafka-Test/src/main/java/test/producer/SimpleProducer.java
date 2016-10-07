package test.producer;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.annotation.Nonnull;

import JavaSessionize.avro.LogLine;
import radar.avro.User;
import test.event.EventGenerator;
import radar.utils.KafkaProperties;

/**
 * Created by Francesco Nobilia on 26/09/2016.
 */
public class SimpleProducer {

    private final static Logger log = Logger.getLogger(SimpleProducer.class);

    private final KafkaProducer producer;

    public SimpleProducer(){
        producer = new KafkaProducer(KafkaProperties.getSimpleProducer());
    }

    /**
     * @return Host IP
     * @throws IllegalArgumentException if the ID has not been generated
     */
    private String getHost() throws IllegalArgumentException {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Client ID cannot be generated."+"\n"+e.getMessage());
        }
    }

    /**
     * Send message to the Kafka topic
     * @param message
     * @param topic
     * @param key
     */
    public void send(@Nonnull String topic, @Nonnull Object key, @Nonnull final Object message){

        if(topic == null || key == null || message == null){
            throw new NullPointerException("You are trying to send a null message");
        }

        ProducerRecord<Object, Object> record = new ProducerRecord<Object, Object>(topic, key, message);

        log.info("["+topic+"]"+key.toString()+" - "+message.toString());

        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null) {
                    log.error("Error sending message ",e);
                } else {
                    log.trace(recordMetadata.toString());
                }
            }
        });
    }

    /**
     * Close the Kafka Producer
     */
    public void shutdown(){
        producer.close();
        log.info("SHUTDOWN");
    }


    /**
     * @return new record serialised by the hardcoded AVRO schema
     */
    public GenericRecord testDataHardcodedSchema(){
        String schemaString = "{\"type\":\"record\",\"name\":\"myrecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";

        Parser parser = new Parser();
        Schema schema = parser.parse(schemaString);

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("f1", dateFormat.format(new Date()).toString());

        return avroRecord;
    }

    /**
     * @return new LogLine serialised by the AVRO schema stored inside resources
     */
    public LogLine testDataResourceSchema(){
        return EventGenerator.getNext();
    }

    /**
     * @return new User serialised by the AVRO schema stored inside resources
     */
    public User testKeyResourceSchema(){
        User user = new User();

        user.setName("Francesco");
        user.setSurname("Nobilia");
        user.setDevice("Mac Book Pro");
        user.setIp(getHost());

        return user;
    }

    /**
     * @return {key-message}={User-LogLine} items have been serialised by the AVRO schema
     * stored inside resources
     */
    public Object[] testResourceSchema(){
        Object[] event = new Object[2];

        event[1] = EventGenerator.getNext();
        event[0] = new User("Francesco","Nobilia","Mac Book Pro",((LogLine) event[1]).getIp());

        return event;
    }

}


