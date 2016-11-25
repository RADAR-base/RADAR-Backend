package org.radarcns.empaticaE4.topic;

import org.radarcns.topic.device.DeviceTopics;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Francesco Nobilia on 18/11/2016.
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

    public E4SensorTopics getSensorTopics() {
        return sensorTopics;
    }

    public E4InternalTopics getInternalTopics() {
        return internalTopics;
    }
}
