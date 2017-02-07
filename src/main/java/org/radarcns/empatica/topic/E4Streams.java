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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Singleton class representing the list of Empatica E4 topics
 */
public final class E4Streams {
    private static final E4Streams INSTANCE = new E4Streams();
    private static final E4SensorStreams SENSOR_TOPICS = E4SensorStreams.getInstance();
    private static final E4InternalStreams INTERNAL_TOPICS = E4InternalStreams.getInstance();

    public static E4Streams getInstance() {
        return INSTANCE;
    }

    private E4Streams(){}

    public List<String> getTopicNames() {
        Set<String> set = new HashSet<>();

        set.addAll(getSensorStreams().getTopicNames());
        set.addAll(getInternalStreams().getTopicNames());

        ArrayList<String> list = new ArrayList<>(set);
        list.sort(String::compareTo);

        return list;
    }

    /**
     * @return an INSTANCE of E4SensorStreams
     */
    public E4SensorStreams getSensorStreams() {
        return SENSOR_TOPICS;
    }

    /**
     * @return an INSTANCE of E4InternalStreams
     */
    public E4InternalStreams getInternalStreams() {
        return INTERNAL_TOPICS;
    }
}
