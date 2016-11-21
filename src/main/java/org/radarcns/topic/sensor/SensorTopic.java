package org.radarcns.topic.sensor;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.radarcns.key.MeasurementKey;
import org.radarcns.topic.avro.AvroTopic;

import java.util.Collection;
import java.util.LinkedList;

import javax.annotation.Nonnull;

/**
 * Created by Francesco Nobilia on 18/11/2016.
 */
public class SensorTopic<V extends SpecificRecord> extends AvroTopic<MeasurementKey,V> {

    public SensorTopic(@Nonnull String name, @Nonnull Class<V> valueClass) {
        super(name,MeasurementKey.class,valueClass);
    }

    public String getInputTopic(){
        return super.getName();
    }

    public Collection<String> getAllTopicNames(){
        Collection<String> collection = new LinkedList<>();

        collection.add(getInputTopic());
        collection.add(getInProgessTopic());
        collection.add(getOutputTopic());

        return collection;
    }

    @Override
    public Serde<MeasurementKey> getKeySerde(){
        return (Serde<MeasurementKey>) super.getKeySerde();
    }
}
