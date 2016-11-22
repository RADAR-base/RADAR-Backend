package org.radarcns.stream.aggregator;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.radarcns.key.MeasurementKey;
import org.radarcns.topic.Internal.InternalTopic;
import org.radarcns.util.KafkaProperties;

import java.io.IOException;

import javax.annotation.Nonnull;

/**
 * Created by Francesco Nobilia on 11/10/2016.
 */
public abstract class InternalAggregator<I,O extends SpecificRecord> implements AggregatorRadar {

    private final String clientID;
    private final KafkaStreams streams;
    private final InternalTopic<O> topic;

    public InternalAggregator(@Nonnull InternalTopic<O> topic, @Nonnull String clientID) throws IOException{
        this.topic = topic;
        this.clientID = clientID;

        streams = new KafkaStreams(getBuilder(), KafkaProperties.getStream(clientID,1));
    }

    public InternalAggregator(@Nonnull InternalTopic<O> topic, @Nonnull String clientID, @Nonnull int numThread) throws IOException{
        if(numThread < 1){
            throw new IllegalStateException("The number of concurrent threads must be bigger than 0");
        }

        this.topic = topic;
        this.clientID = clientID;

        streams = new KafkaStreams(getBuilder(), KafkaProperties.getStream(clientID,numThread));
    }

    private KStreamBuilder getBuilder() throws IOException{

        final KStreamBuilder builder = new KStreamBuilder();

        KStream<MeasurementKey,I> valueKStream =  builder.stream(topic.getInputTopic());

        setStream(valueKStream, topic);

        return builder;
    }

    protected abstract void setStream(KStream<MeasurementKey,I> kstream, InternalTopic<O> topic) throws IOException;

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

