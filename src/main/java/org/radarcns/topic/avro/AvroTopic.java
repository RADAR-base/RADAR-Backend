package org.radarcns.topic.avro;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.radarcns.key.MeasurementKey;
import org.radarcns.stream.collector.DoubleValueCollector;
import org.radarcns.util.serde.RadarSerde;

import java.util.Collection;

import javax.annotation.Nonnull;

/**
 * Set of Avro Topics
 * The generic K is the topic key
 * The generic V is the topic record
 * It defines:<ul>
 * <li>a source topic containing collected data(e.g. input topic)</li>
 * <li>a topic where temporary results are stored before the end of the time window (e.g. in_progress)</li>
 * <li>an output topic that persists the aggregated results (e.g. input topic)</li>
 */
public abstract class AvroTopic <K extends SpecificRecord, V extends SpecificRecord> {
    private final String name;
    private final Class<K> keyClass;
    private final Class<V> valueClass;

    //Enumerate all possible suffix
    private enum Suffix {
        output("output"), store("store");

        private final String param;

        Suffix(String param) {
            this.param = param;
        }

        public String getParam() {
            return param;
        }
    }

    /**
     * @param name the topic name inside the Kafka cluster
     * @param keyClass the java class representing the key
     * @param valueClass the java class representing the record
     */
    public AvroTopic(@Nonnull String name, @Nonnull Class<K> keyClass, @Nonnull Class<V> valueClass) {
        this.name = name;
        this.valueClass = valueClass;
        this.keyClass = keyClass;
    }

    /**
     * @return the topic name
     */
    protected String getName(){
        return this.name;
    }

    /**
     * @return the name of the Input topic
     */
    public abstract String getInputTopic();

    /**
     * @return the name of the topic used to write results of data aggregation
     */
    public String getOutputTopic(){
        return name+"_"+ Suffix.output;
    }

    /**
     * @return the State Store name for the given topic
     */
    public String getStateStoreName(){
        return name+"_"+ Suffix.store;
    }

    /**
     * @return a Serde that can be applied to the key object of the input topic
     */
    public Serde<? extends SpecificRecord> getKeySerde(){
        return new RadarSerde<>(keyClass).getSerde();
    }

    /**
     * @return the collection of all used topic
     */
    public abstract Collection<String> getAllTopicNames();
}
