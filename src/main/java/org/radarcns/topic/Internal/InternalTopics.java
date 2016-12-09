package org.radarcns.topic.internal;

import org.apache.avro.specific.SpecificRecord;

import java.util.Set;

/**
 * Created by Francesco Nobilia on 17/11/2016.
 */
public interface InternalTopics {
    InternalTopic<? extends SpecificRecord> getTopic(String name);

    Set<String> getTopicNames();
}
