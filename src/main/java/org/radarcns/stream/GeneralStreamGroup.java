/*
 * Copyright 2017 The Hyve and King's College London
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

package org.radarcns.stream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.radarcns.topic.KafkaTopic;

/**
 * Implementation of a {@link StreamGroup}. Override to create specific streams for a given
 * device: use the {@link #createStream(String, String)} to create an internal stream and
 * {@link #createSensorStream(String)} to create a sensor stream.
 *
 * <p>To access the streams, create getter functions or use the {@link #getStreamDefinition(String)}
 * method.
 */
public class GeneralStreamGroup implements StreamGroup {

    public static final String OUTPUT_LABEL = "_output";

    private final Map<String, StreamDefinition> topicMap;
    private final Set<String> topicNames;

    public GeneralStreamGroup() {
        topicMap = new HashMap<>();
        topicNames = new HashSet<>();
    }

    /**
     * Create a stream from input to output topic. By using this method, {@link #getTopicNames()}
     * and {@link #getStreamDefinition(String)} automatically get updated.
     * @param input input topic name
     * @param output output topic name
     * @return stream definition.
     */
    protected StreamDefinition createStream(String input, String output) {
        StreamDefinition ret = new StreamDefinition(new KafkaTopic(input), new KafkaTopic(output));
        topicMap.put(input, ret);
        topicNames.add(input);
        topicNames.add(output);
        return ret;
    }

    /**
     * Create a sensor stream from input topic to a "[input]_output" topic. By using this method,
     * {@link #getTopicNames()} and {@link #getStreamDefinition(String)} automatically get updated.
     * @param input input topic name
     * @return sensor stream definition
     */
    protected StreamDefinition createSensorStream(String input) {
        return createStream(input, input + OUTPUT_LABEL);
    }

    @Override
    public StreamDefinition getStreamDefinition(String inputTopic) {
        StreamDefinition topic = topicMap.get(inputTopic);
        if (topic == null) {
            throw new IllegalArgumentException("Topic " + inputTopic + " unknown");
        }
        return topic;
    }

    @Override
    public List<String> getTopicNames() {
        List<String> topicList = new ArrayList<>(topicNames);
        topicList.sort(String.CASE_INSENSITIVE_ORDER);
        return topicList;
    }
}
