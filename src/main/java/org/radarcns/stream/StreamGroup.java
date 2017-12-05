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

import java.util.Collection;
import java.util.List;

/**
 * A group of Kafka stream definitions. For example, a stream group can be created for a type of
 * device or a business case.
 */
public interface StreamGroup {
    /**
     * Get all topic names, input and output, that are defined in this stream group.
     * @return alphabetically ordered list of topic names
     */
    List<String> getTopicNames();

    /**
     * Get the stream definition for a given input topic.
     * @param inputTopic input topic name
     * @return stream definition of given input topic
     * @throws IllegalArgumentException if the input topic is not found.
     */
    Collection<StreamDefinition> getStreamDefinition(String inputTopic);
}
