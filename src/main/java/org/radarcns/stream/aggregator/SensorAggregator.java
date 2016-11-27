package org.radarcns.stream.aggregator;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.radarcns.config.KafkaProperty;
import org.radarcns.key.MeasurementKey;
import org.radarcns.topic.sensor.SensorTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.Nonnull;

/**
 * Created by Francesco Nobilia on 11/10/2016.
 */
public abstract class SensorAggregator<V extends SpecificRecord> implements AggregatorWorker {

    private final static Logger log = LoggerFactory.getLogger(SensorAggregator.class);

    private final String clientID;
    private KafkaStreams streams;
    private final SensorTopic<V> topic;

    private final MasterAggregator master;

    public SensorAggregator(@Nonnull SensorTopic<V> topic, @Nonnull String clientID, @Nonnull MasterAggregator master) throws IOException{
        this(topic,clientID,1,master);
    }

    public SensorAggregator(@Nonnull SensorTopic<V> topic, @Nonnull String clientID, @Nonnull int numThread, @Nonnull MasterAggregator master) throws IOException{
        if(numThread < 1){
            throw new IllegalStateException("The number of concurrent threads must be bigger than 0");
        }

        this.topic = topic;
        this.clientID = clientID;
        this.master = master;

        streams = new KafkaStreams(getBuilder(), KafkaProperty.getStream(clientID,numThread,DeviceTimestampExtractor.class));

        log.info("Creating {} stream",clientID);
    }

    private KStreamBuilder getBuilder() throws IOException{
        KStreamBuilder builder = new KStreamBuilder();

        log.trace(topic.getInputTopic());

        KStream<MeasurementKey,V> valueKStream =  builder.stream(topic.getInputTopic());

        setStream(valueKStream, topic);

        log.info("Creating the builder for {} stream",clientID);

        return builder;
    }

    protected abstract void setStream(KStream<MeasurementKey,V> kstream, SensorTopic<V> topic) throws IOException;

    @Override
    public void run() {
        log.info("Starting {} stream",clientID);
        streams.start();

        master.notifyStartedStream(clientID);
    }

    @Override
    public void shutdown(){
        log.info("Shutting down {} stream",clientID);
        streams.close();

        master.notifyClosedStream(clientID);
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
        Thread thread;

        thread = new Thread(this);
        thread.setName(this.clientID);

        return thread;
    }
}
