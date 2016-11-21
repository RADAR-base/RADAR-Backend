package org.radarcns.empaticaE4.topic;

import org.apache.avro.specific.SpecificRecord;
import org.radarcns.aggregator.DoubleAggegator;
import org.radarcns.topic.Internal.InternalTopic;
import org.radarcns.topic.Internal.InternalTopics;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Francesco Nobilia on 17/11/2016.
 */
public class E4InternalTopics implements InternalTopics {

    private final InternalTopic<DoubleAggegator> heartRateTopic;

    private final static Object syncObject = new Object();
    private static E4InternalTopics instance = null;

    protected static E4InternalTopics getInstance() {
        synchronized (syncObject) {
            if (instance == null) {
                instance = new E4InternalTopics();
            }
            return instance;
        }
    }

    private E4InternalTopics() {
        heartRateTopic = new InternalTopic<>("android_empatica_e4_inter_beat_interval","android_empatica_e4_heartrate", DoubleAggegator.class);
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

    public InternalTopic<DoubleAggegator> getHeartRateTopic() {
        return heartRateTopic;
    }

}
