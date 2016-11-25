package org.radarcns.util.serde;

import org.apache.kafka.common.serialization.Serde;
import org.radarcns.key.MeasurementKey;
import org.radarcns.stream.collector.DoubleArrayCollector;
import org.radarcns.stream.collector.DoubleValueCollector;

/**
 * Created by Francesco Nobilia on 17/11/2016.
 */
public class RadarSerdes{

    private final Serde<MeasurementKey> measurementKeySerde;

    private final Serde<DoubleValueCollector> doubelCollector;
    private final Serde<DoubleArrayCollector> doubelArrayCollector;

    private static RadarSerdes instance = new RadarSerdes();

    public static RadarSerdes getInstance() {
        return instance;
    }

    private RadarSerdes() {
        measurementKeySerde = new RadarSerde<>(MeasurementKey.class).getSerde();
        doubelCollector = new RadarSerde<>(DoubleValueCollector.class).getSerde();
        doubelArrayCollector = new RadarSerde<>(DoubleArrayCollector.class).getSerde();
    }

    public Serde<MeasurementKey> getMeasurementKeySerde() {
        return measurementKeySerde;
    }

    public Serde<DoubleValueCollector> getDoubelCollector() {
        return doubelCollector;
    }

    public Serde<DoubleArrayCollector> getDoubelArrayCollector() {
        return doubelArrayCollector;
    }
}
