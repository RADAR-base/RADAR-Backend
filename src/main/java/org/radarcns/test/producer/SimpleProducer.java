package org.radarcns.test.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.radarcns.util.KafkaProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

import javax.annotation.Nonnull;

/**
 * Created by Francesco Nobilia on 26/09/2016.
 */
public class SimpleProducer {

    private final static Logger log = LoggerFactory.getLogger(SimpleProducer.class);

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

}


