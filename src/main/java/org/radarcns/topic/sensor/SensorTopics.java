package org.radarcns.topic.sensor;

import org.apache.avro.specific.SpecificRecord;

import java.util.Set;

public interface SensorTopics {
    SensorTopic<? extends SpecificRecord> getTopic(String name);

    Set<String> getTopicNames();
}
