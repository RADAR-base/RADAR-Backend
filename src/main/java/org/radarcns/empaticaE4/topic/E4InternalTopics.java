package org.radarcns.empaticaE4.topic;

import org.apache.avro.specific.SpecificRecord;
import org.radarcns.aggregator.DoubleAggregator;
import org.radarcns.topic.internal.InternalTopic;
import org.radarcns.topic.internal.InternalTopics;

import java.util.HashSet;
import java.util.Set;

/**
 * Entire set of Empatica E4 InternalTopic
 * @see org.radarcns.topic.internal.InternalTopic
 */
public class E4InternalTopics implements InternalTopics {
    private final InternalTopic<DoubleAggregator> heartRateTopic;

    private static E4InternalTopics instance = new E4InternalTopics();

    protected static E4InternalTopics getInstance() {
        return instance;
    }

    private E4InternalTopics() {
        heartRateTopic = new InternalTopic<>(
                "android_empatica_e4_inter_beat_interval",
                "android_empatica_e4_heartrate", DoubleAggregator.class);
    }

    @Override
    public InternalTopic<? extends SpecificRecord> getTopic(String name) {
        switch (name) {
            case "android_empatica_e4_heartrate":
                return heartRateTopic;
            default:
                throw new IllegalArgumentException("Topic " + name + " unknown");
        }
    }

    @Override
    public Set<String> getTopicNames() {
        Set<String> set = new HashSet<>();

        set.addAll(heartRateTopic.getAllTopicNames());

        return set;
    }

    public InternalTopic<DoubleAggregator> getHeartRateTopic() {
        return heartRateTopic;
    }

}
