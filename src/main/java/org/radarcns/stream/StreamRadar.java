package org.radarcns.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.io.IOException;

import org.radarcns.util.KafkaProperties;

/**
 * Created by Francesco Nobilia on 11/10/2016.
 */
public abstract class StreamRadar implements Runnable{

    private final KafkaStreams streams;

    public StreamRadar() throws IOException{
        streams = new KafkaStreams(getBuilder(), KafkaProperties.getStream());

    }

    public StreamRadar(String clientID) throws IOException{
        streams = new KafkaStreams(getBuilder(), KafkaProperties.getStream(clientID));

    }

    public abstract KStreamBuilder getBuilder() throws IOException;

    @Override
    public void run() {
        streams.start();
    }

    public void shutdown(){
        streams.close();
    }

}
