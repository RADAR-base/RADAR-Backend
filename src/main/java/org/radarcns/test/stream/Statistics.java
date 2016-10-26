package org.radarcns.test.stream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;

import org.radarcns.empaticaE4.EmpaticaE4InterBeatInterval;
import org.radarcns.key.MeasurementKey;
import org.radarcns.stream.StreamRadar;

import java.io.IOException;
import org.radarcns.stream.ValueCollector;
import org.radarcns.util.RadarUtils;
import org.radarcns.util.serde.RadarSerde;
import org.radarcns.util.serde.SpecificAvroSerde;

/**
 * Created by Francesco Nobilia on 11/10/2016.
 */
public class Statistics extends StreamRadar {

    public Statistics() throws IOException{
        super();
    }

    public Statistics(String clientID) throws IOException{
        super(clientID);
    }

    @Override
    public KStreamBuilder getBuilder() throws IOException{

        Serde<ValueCollector> collectorSerde = new RadarSerde<>(ValueCollector.class).getSerde();
        Serde<MeasurementKey> keySerde = new RadarSerde<>(MeasurementKey.class).getSerde();

        final KStreamBuilder builder = new KStreamBuilder();

        KStream<MeasurementKey,EmpaticaE4InterBeatInterval> valueKStream =  builder.stream("input-statistic");
        valueKStream.aggregateByKey(ValueCollector::new,
                        (k, v, valueCollector) -> valueCollector.add(RadarUtils.ibiToHR(v.getInterBeatInterval())),
                        TimeWindows.of("value-summaries", 10000),
                keySerde,collectorSerde)
                .toStream()
                .map((k,v) -> new KeyValue<>(RadarUtils.getWindowed(k),v.convertInAvro()))
                .to("output-statistic");

        return builder;
    }
}
