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

import org.apache.avro.specific.SpecificRecord;

import java.util.Set;

/**
 * Created by Francesco Nobilia on 17/11/2016.
 */
public interface InternalTopics {
    InternalTopic<? extends SpecificRecord> getTopic(String name);

    Set<String> getTopicNames();
}
