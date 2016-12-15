package org.radarcns.topic.sensor;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.radarcns.key.MeasurementKey;
import org.radarcns.topic.avro.AvroTopic;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

import javax.annotation.Nonnull;

/**
 * Specialisation of AvroTopic representing topics used to aggregate data. Since it describes the
 * computation from the initial point of view. The topic key is org.radarcns.key.MeasurementKey
 *
 * @see org.radarcns.topic.avro.AvroTopic
 * @see org.radarcns.key.MeasurementKey
 */
public class SensorTopic<V extends SpecificRecord> extends AvroTopic<MeasurementKey, V> {
    /**
     * @param name: name of the input topic
     * @param valueClass: java class representing the record
     */
    public SensorTopic(@Nonnull String name, @Nonnull Class<V> valueClass) {
        super(name, MeasurementKey.class, valueClass);
    }
}
