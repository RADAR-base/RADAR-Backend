/*
 * Copyright 2017 Kings College London and The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.empatica.topic;

import org.radarcns.topic.DeviceTopics;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Singleton class representing the list of Empatica E4 topics
 */
public final class E4Topics implements DeviceTopics {
    private static E4Topics instance = new E4Topics();

    private static final E4SensorTopics SENSOR_TOPICS = E4SensorTopics.getInstance();
    private static final E4InternalTopics INTERNAL_TOPICS = E4InternalTopics.getInstance();

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
        return SENSOR_TOPICS;
    }

    /**
     * @return an instance of E4InternalTopics
     */
    public E4InternalTopics getInternalTopics() {
        return INTERNAL_TOPICS;
    }
}
