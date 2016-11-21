package org.radarcns.empaticaE4.topic;

import org.radarcns.topic.device.DeviceTopics;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Francesco Nobilia on 18/11/2016.
 */
public class E4Topics implements DeviceTopics {

    private final static Object syncObject = new Object();
    private static E4Topics instance = null;

    private static E4SensorTopics sensorTopics = null;
    private static E4InternalTopics internalTopics = null;

    public static E4Topics getInstance() {
        synchronized (syncObject) {
            if (instance == null) {
                instance = new E4Topics();
            }
            return instance;
        }
    }

    @Override
    public Set<String> getTopicNames() {
        Set<String> set = new HashSet<>();

        set.addAll(E4InternalTopics.getInstance().getTopicNames());
        set.addAll(E4SensorTopics.getInstance().getTopicNames());

        return set;
    }

    public E4SensorTopics getSensorTopics() {
        synchronized (syncObject) {
            if (sensorTopics == null) {
                sensorTopics = E4SensorTopics.getInstance();
            }
            return sensorTopics;
        }
    }

    public E4InternalTopics getInternalTopics() {
        synchronized (syncObject) {
            if (internalTopics == null) {
                internalTopics = E4InternalTopics.getInstance();
            }
            return internalTopics;
        }
    }
}
