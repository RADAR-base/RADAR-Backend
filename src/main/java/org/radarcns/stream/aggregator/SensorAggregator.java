package org.radarcns.stream.aggregator;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.radarcns.key.MeasurementKey;
import org.radarcns.topic.sensor.SensorTopic;
import org.radarcns.util.KafkaProperties;

import java.io.IOException;

import javax.annotation.Nonnull;

/**
 * Created by Francesco Nobilia on 11/10/2016.
 */
public abstract class SensorAggregator<V extends SpecificRecord> implements AggregatorRadar {

    private final String clientID;
    private final KafkaStreams streams;
    private final SensorTopic<V> topic;

    public SensorAggregator(@Nonnull SensorTopic<V> topic, @Nonnull String clientID) throws IOException{
        this.topic = topic;
        this.clientID = clientID;

        streams = new KafkaStreams(getBuilder(), KafkaProperties.getStream(clientID));
    }

    private KStreamBuilder getBuilder() throws IOException{

        final KStreamBuilder builder = new KStreamBuilder();

        KStream<MeasurementKey,V> valueKStream =  builder.stream(topic.getInputTopic());

        setStream(valueKStream, topic);

        return builder;
    }

    protected abstract void setStream(KStream<MeasurementKey,V> kstream, SensorTopic<V> topic) throws IOException;

    @Override
    public void run() {
        streams.start();
    }

    @Override
    public void shutdown(){
        streams.close();
    }

    @Override
    public String getClientID(){
        return this.clientID;
    }

    @Override
    public Thread getThread() {
        Thread thread;

        thread = new Thread(this);
        thread.setName(this.clientID);

        return thread;
    }
}
