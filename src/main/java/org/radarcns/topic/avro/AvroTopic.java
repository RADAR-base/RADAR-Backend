package org.radarcns.topic.avro;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.radarcns.key.MeasurementKey;
import org.radarcns.stream.collector.DoubleValueCollector;
import org.radarcns.util.serde.RadarSerde;

import java.util.Collection;

import javax.annotation.Nonnull;

/**
 * Created by Francesco Nobilia on 17/11/2016.
 */
public abstract class AvroTopic <K extends SpecificRecord, V extends SpecificRecord> {
    private final String name;
    private final Class<K> keyClass;
    private final Class<V> valueClass;

    //private final Serde<K> keySerde;
    private final Serde<DoubleValueCollector> doubelCollectorSerde = new RadarSerde<>(DoubleValueCollector.class).getSerde();
    private final Serde<MeasurementKey> measurementKeySerde = new RadarSerde<>(MeasurementKey.class).getSerde();

    //Enumerate all possible topics suffix
    private enum Suffix {
        in_progress("in_progress"), output("output");

        private final String param;

        Suffix(String param) {
            this.param = param;
        }

        public String getParam() {
            return param;
        }
    }

    public AvroTopic(@Nonnull String name, @Nonnull Class<K> keyClass, @Nonnull Class<V> valueClass) {
        this.name = name;
        this.valueClass = valueClass;
        this.keyClass = keyClass;
    }

    protected String getName(){
        return this.name;
    }

    public Class<K> getKeyClass() {
        return keyClass;
    }

    public Class<V> getValueClass() {
        return valueClass;
    }

    public abstract String getInputTopic();

    public String getInProgessTopic(){
        return name+"_"+ Suffix.in_progress;
    }

    public String getOutputTopic(){
        return name+"_"+ Suffix.output;
    }

    public Serde<? extends SpecificRecord> getKeySerde(){
        return new RadarSerde<>(keyClass).getSerde();
    }

    public Serde<DoubleValueCollector> getDoubleCollectorSerde() {
        return doubelCollectorSerde;
    }

    public Serde<MeasurementKey> getMeasurementKeySerde() {
        return measurementKeySerde;
    }

    public abstract Collection<String> getAllTopicNames();
}
