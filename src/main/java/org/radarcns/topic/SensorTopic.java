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
import org.radarcns.key.MeasurementKey;

import javax.annotation.Nonnull;

/**
 * Specialisation of AvroTopic representing topics used to aggregate data. Since it describes the
 * computation from the initial point of view. The topic key is {@link MeasurementKey}.
 *
 * @param <V> type of record value.
 * @see AvroTopic
 * @see org.radarcns.key.MeasurementKey
 */
public class SensorTopic<V extends SpecificRecord> extends AvroTopic<MeasurementKey, V> {
    /**
     * @param name name of the input topic
     */
    public SensorTopic(@Nonnull String name) {
        super(name);
    }
}
