package org.radarcns.empatica.topic;

import org.radarcns.topic.DeviceTopics;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Singleton class representing the list of Empatica E4 topics
 */
public class E4Topics implements DeviceTopics {

    private static E4Topics instance = new E4Topics();

    private static final E4SensorTopics sensorTopics = E4SensorTopics.getInstance();
    private static final E4InternalTopics internalTopics = E4InternalTopics.getInstance();

    public static E4Topics getInstance() {
        return instance;
    }

    private E4Topics(){}

    @Override
    public List<String> getTopicNames() {
        Set<String> set = new HashSet<>();

        set.addAll(E4InternalTopics.getInstance().getTopicNames());
        set.addAll(E4SensorTopics.getInstance().getTopicNames());

        ArrayList<String> list = new ArrayList<>(set);
        list.sort(String::compareTo);

        return list;
    }

    /**
     * @return an instance of E4SensorTopics
     */
    public E4SensorTopics getSensorTopics() {
        return sensorTopics;
    }

    /**
     * @return an instance of E4InternalTopics
     */
    public E4InternalTopics getInternalTopics() {
        return internalTopics;
    }
}
