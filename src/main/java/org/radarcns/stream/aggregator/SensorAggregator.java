package org.radarcns.stream.aggregator;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.log4j.Logger;
import org.radarcns.key.MeasurementKey;
import org.radarcns.topic.sensor.SensorTopic;
import org.radarcns.util.KafkaProperties;

import java.io.IOException;

import javax.annotation.Nonnull;

/**
 * Created by Francesco Nobilia on 11/10/2016.
 */
public abstract class SensorAggregator<V extends SpecificRecord> implements AggregatorRadar {

    private final static Logger log = Logger.getLogger(SensorAggregator.class);

    private final String clientID;
    private KafkaStreams streams;
    private final SensorTopic<V> topic;

    public SensorAggregator(@Nonnull SensorTopic<V> topic, @Nonnull String clientID) throws IOException{
        log.trace("Init");
        this.topic = topic;
        this.clientID = clientID;

        streams = new KafkaStreams(getBuilder(), KafkaProperties.getStream(clientID,1));

        log.trace("Finish");
    }

    public SensorAggregator(@Nonnull SensorTopic<V> topic, @Nonnull String clientID, @Nonnull int numThread) throws IOException{
        log.trace("Init");

        if(numThread < 1){
            throw new IllegalStateException("The number of concurrent threads must be bigger than 0");
        }

        this.topic = topic;
        this.clientID = clientID;

        streams = new KafkaStreams(getBuilder(), KafkaProperties.getStream(clientID,numThread));

        log.trace("Finish");
    }

    private KStreamBuilder getBuilder() throws IOException{
        log.trace("Init");
        KStreamBuilder builder = new KStreamBuilder();

        log.trace(topic.getInputTopic());

        KStream<MeasurementKey,V> valueKStream =  builder.stream(topic.getInputTopic());

        setStream(valueKStream, topic);

        log.trace("Finish");
        return builder;
    }

    protected abstract void setStream(KStream<MeasurementKey,V> kstream, SensorTopic<V> topic) throws IOException;

    @Override
    public void run() {
        log.trace("Init");
        streams.start();
        log.trace("Finish");
    }

    @Override
    public void shutdown(){
        log.trace("Init");
        streams.close();
        log.trace("Finish");
    }

    @Override
    public String getClientID(){
        return this.clientID;
    }

    @Override
    public String getName(){
        return getClientID();
    }

    @Override
    public Thread getThread() {
        log.trace("Init");
        Thread thread;

        thread = new Thread(this);
        thread.setName(this.clientID);

        log.trace("Finish");
        return thread;
    }
}
