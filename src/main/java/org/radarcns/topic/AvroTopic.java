package org.radarcns.topic;

import org.apache.avro.specific.SpecificRecord;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;

/**
 * Set of Avro Topics
 * It defines:<ul>
 * <li>a source topic containing collected data(e.g. input topic)</li>
 * <li>a topic where temporary results are stored before the end of the time window
 *     (e.g. in_progress)</li>
 * <li>an output topic that persists the aggregated results (e.g. input topic)</li>
 * </ul>
 * @param <K> topic key type
 * @param <V> topic record type
 */
public class AvroTopic<K extends SpecificRecord, V extends SpecificRecord> {
    private final String name;

    /** Topic suffixes for different use cases. */
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
     * @param name topic name inside the Kafka cluster
     */
    public AvroTopic(@Nonnull String name) {
        this.name = name;
    }

    /**
     * @return topic name
     */
    protected String getName() {
        return this.name;
    }

    /**
     * @return name of the Input topic
     */
    public String getInputTopic() {
        return this.name;
    }

    /**
     * @return name of the topic used to write results of data aggregation
     */
    public String getOutputTopic() {
        return name + "_" + Suffix.output;
    }

    /**
     * @return State Store name for the given topic
     */
    public String getStateStoreName() {
        return name + "_" + Suffix.store;
    }

    /**
     * @return collection of all used topic
     */
    public Collection<String> getAllTopicNames() {
        return Arrays.asList(getInputTopic(), getOutputTopic());
    }
}
