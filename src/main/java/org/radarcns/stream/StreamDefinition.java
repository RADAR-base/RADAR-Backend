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

import org.radarcns.topic.KafkaTopic;

public class StreamDefinition {

    public static final String FROM_LABEL = "From-";
    public static final String TO_LABEL = "-To-";

    private final KafkaTopic inputTopic;
    private final KafkaTopic outputTopic;

    /**
     * Constructor. It takes in input the topic name to be consumed and to topic name where the
     *      related stream will write the computed values.
     *
     * @param input source {@link KafkaTopic}
     * @param output output {@link KafkaTopic}
     */
    public StreamDefinition(KafkaTopic input, KafkaTopic output) {
        if (input == null || output == null) {
            throw new IllegalArgumentException("Input and output topic may not be null");
        }
        this.inputTopic = input;
        this.outputTopic = output;
    }

    public KafkaTopic getInputTopic() {
        return inputTopic;
    }

    public KafkaTopic getOutputTopic() {
        return outputTopic;
    }

    /**
     * Kafka Streams allows for stateful stream processing. The internal state is managed in
     *      so-called state stores. A fault-tolerant state store is an internally created and
     *      compacted changelog topic. This function return the changelog topic name.
     *
     * @return {@code String} representing the changelog topic name
     */
    public String getStateStoreName() {
        return FROM_LABEL + getInputTopic().getName() + TO_LABEL + getOutputTopic().getName();
    }
}
