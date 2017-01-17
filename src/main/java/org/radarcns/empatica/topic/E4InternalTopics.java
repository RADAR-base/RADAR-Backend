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

import org.apache.avro.specific.SpecificRecord;
import org.radarcns.aggregator.DoubleAggregator;
import org.radarcns.topic.InternalTopic;
import org.radarcns.topic.InternalTopics;

import java.util.HashSet;
import java.util.Set;

/**
 * Entire set of Empatica E4 InternalTopic
 * @see InternalTopic
 */
public final class E4InternalTopics implements InternalTopics {
    private final InternalTopic<DoubleAggregator> heartRateTopic;

    private static E4InternalTopics instance = new E4InternalTopics();

    static E4InternalTopics getInstance() {
        return instance;
    }

    private E4InternalTopics() {
        heartRateTopic = new InternalTopic<>(
                "android_empatica_e4_inter_beat_interval",
                "android_empatica_e4_heartrate");
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
