package org.radarcns.topic;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CombinedStreamGroup implements StreamGroup {
    @Override
    public List<String> getTopicNames() {
        Set<String> set = new HashSet<>();

        set.addAll(getSensorStreams().getTopicNames());
        set.addAll(getInternalStreams().getTopicNames());

        ArrayList<String> list = new ArrayList<>(set);
        list.sort(String::compareTo);

        return list;
    }

    /**
     * Get sensor streams. In this implementation, null is returned. Override to get a stream group
     * @return null or the stream group associated to a raw sensor
     */
    public StreamGroup getSensorStreams() {
        return null;
    }

    /**
     * Get internal streams. In this implementation, null is returned. Override to get a stream
     * group.
     *
     * @return null or the stream group associated to internal topic processing.
     */
    public StreamGroup getInternalStreams() {
        return null;
    }
}
