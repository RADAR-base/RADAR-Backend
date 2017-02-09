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

package org.radarcns.topic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeneralStreamGroup implements StreamGroup {
    private final Map<String, StreamDefinition> topicMap;
    private final List<String> topicNames;

    public GeneralStreamGroup() {
        topicMap = new HashMap<>();
        topicNames = new ArrayList<>();
    }

    protected StreamDefinition createStream(String input, String output) {
        StreamDefinition ret = new StreamDefinition(new KafkaTopic(input), new KafkaTopic(output));
        topicMap.put(input, ret);
        topicNames.add(input);
        topicNames.add(output);
        topicNames.sort(String::compareTo);
        return ret;
    }

    public StreamDefinition getStreamDefinition(String name) {
        StreamDefinition topic = topicMap.get(name);
        if (topic == null) {
            throw new IllegalArgumentException("Topic " + name + " unknown");
        }
        return topic;
    }

    @Override
    public List<String> getTopicNames() {
        return topicNames;
    }
}
