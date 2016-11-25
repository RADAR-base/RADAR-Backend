package org.radarcns.topic.Internal;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.radarcns.key.MeasurementKey;
import org.radarcns.key.WindowedKey;
import org.radarcns.topic.avro.AvroTopic;
import org.radarcns.util.serde.RadarSerde;

import java.util.Collection;
import java.util.LinkedList;

import javax.annotation.Nonnull;

/**
 * Created by Francesco Nobilia on 18/11/2016.
 */
public class InternalTopic<V extends SpecificRecord> extends AvroTopic<WindowedKey,V> {

    private final String source;

    public InternalTopic(@Nonnull String source, @Nonnull String name, @Nonnull Class<V> valueClass) {
        super(name,WindowedKey.class,valueClass);

        this.source = source;
    }

    @Override
    public String getInputTopic(){
        return this.source;
    }

    @Override
    public Collection<String> getAllTopicNames(){
        Collection<String> collection = new LinkedList<>();

        collection.add(source);
        //collection.add(getInProgessTopic());
        collection.add(getOutputTopic());

        return collection;
    }

    @Override
    public Serde<? extends SpecificRecord> getKeySerde(){
        return new RadarSerde<>(MeasurementKey.class).getSerde();
    }


}
