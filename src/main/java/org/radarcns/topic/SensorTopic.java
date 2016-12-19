package org.radarcns.topic;

import org.apache.avro.specific.SpecificRecord;
import org.radarcns.key.MeasurementKey;

import javax.annotation.Nonnull;

/**
 * Specialisation of AvroTopic representing topics used to aggregate data. Since it describes the
 * computation from the initial point of view. The topic key is {@link MeasurementKey}.
 *
 * @param <V> type of record value.
 * @see AvroTopic
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
