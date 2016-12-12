package org.radarcns.util.serde;

import org.apache.kafka.common.serialization.Serde;
import org.radarcns.key.MeasurementKey;
import org.radarcns.stream.collector.DoubleArrayCollector;
import org.radarcns.stream.collector.DoubleValueCollector;

/**
 * Set of Serde usefull for Kafka Streams
 */
public class RadarSerdes{

    private final Serde<DoubleValueCollector> doubelCollector;
    private final Serde<DoubleArrayCollector> doubelArrayCollector;

    private static RadarSerdes instance = new RadarSerdes();

    public static RadarSerdes getInstance() {
        return instance;
    }

    private RadarSerdes() {
        doubelCollector = new RadarSerde<>(DoubleValueCollector.class).getSerde();
        doubelArrayCollector = new RadarSerde<>(DoubleArrayCollector.class).getSerde();
    }

    public Serde<DoubleValueCollector> getDoubelCollector() {
        return doubelCollector;
    }

    public Serde<DoubleArrayCollector> getDoubelArrayCollector() {
        return doubelArrayCollector;
    }
}
