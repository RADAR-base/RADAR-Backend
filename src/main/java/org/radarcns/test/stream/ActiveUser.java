package org.radarcns.test.stream;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.radarcns.stream.StreamRadar;

/**
 * Created by Francesco Nobilia on 11/10/2016.
 */
public class ActiveUser extends StreamRadar {

    public ActiveUser() throws IOException{
        super();
    }

    public ActiveUser(String clientID) throws IOException{
        super(clientID);
    }

    @Override
    public KStreamBuilder getBuilder() throws IOException{
        //return getNoWindow();
        return getWindow();
    }

    private KStreamBuilder getNoWindow(){
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<GenericRecord, GenericRecord> sessions = builder.stream("sessionized_clicks");

        KStream<String, Long> samplesCount = sessions
                .map((key, value) -> new KeyValue<>(value.get("ip").toString(), value.get("ip").toString()))
                .countByKey(stringSerde, "Counts")
                .toStream();

        samplesCount.to(stringSerde, longSerde, "streams-wordcount-output");

        return builder;
    }

    private KStreamBuilder getWindow(){
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<GenericRecord, GenericRecord> sessions = builder.stream("sessionized_clicks");

        final long timeWindow = 10000; //min*sec*millisec

        sessions.map((k, v) -> KeyValue.pair(v.get("ip").toString(), v))
                .countByKey(TimeWindows.of("radar-window", timeWindow), stringSerde)
                //Change the key adding the information for the current window
                //.toStream((k, v) -> String.format("%s %s", frame(k.window()), k.key()))
                .toStream((k, v) -> String.format("%s", k.key()))
                .to(stringSerde, longSerde, "streams-wordcount-output");

        return builder;
    }

    private String frame(Window window){
        Date start = new Date(TimeUnit.SECONDS.toSeconds(window.start()));
        String formattedStart = new SimpleDateFormat("HH:mm:ss").format(start);

        Date end = new Date(TimeUnit.SECONDS.toSeconds(window.end()));
        String formattedEnd = new SimpleDateFormat("HH:mm:ss").format(end);

        return "["+formattedStart+"-"+formattedEnd+"]";
    }
}
