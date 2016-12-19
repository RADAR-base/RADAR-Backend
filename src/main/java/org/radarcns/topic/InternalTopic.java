package org.radarcns.topic;

import org.apache.avro.specific.SpecificRecord;
import org.radarcns.key.WindowedKey;

import javax.annotation.Nonnull;

/**
 * Specialisation of AvroTopic representing topics used to transform and aggregate data.
 * For instance, the Empatica E4 Heart Rate is an Interal topic. Starting from data in Inter Beat
 * Interval topic, we transform it in heart rate computing (60 / ibi), the results are then aggregated
 * and stored first inside in_progress topic and then in the output topic.
 * Since it describes the computation from the final point of view. The topic key is org.radarcns.key.WindowedKey
 * @see AvroTopic
 * @see org.radarcns.key.WindowedKey
 * @see org.radarcns.key.MeasurementKey
 */
public class InternalTopic<V extends SpecificRecord> extends AvroTopic<WindowedKey, V> {

    private final String source;

    /**
     * @param source: name of the inout topic
     * @param name: name of the output topic
     * @param valueClass: java class representing the record
     */
    public InternalTopic(@Nonnull String source, @Nonnull String name, @Nonnull Class<V> valueClass) {
        super(name,WindowedKey.class,valueClass);

        this.source = source;
    }

    /**
     * @return the input topic. While org.radarcns.topic.SensorTopic uses the varibale name
     * to generate the input topic, Internal topic uses the source variable.
     */
    @Override
    public String getInputTopic(){
        return this.source;
    }
}
