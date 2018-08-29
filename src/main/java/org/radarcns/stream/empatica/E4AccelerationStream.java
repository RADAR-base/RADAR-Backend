/*
 * Copyright 2017 King's College London and The Hyve
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

package org.radarcns.stream.empatica;

import javax.annotation.Nonnull;
import org.apache.kafka.streams.kstream.KStream;
import org.radarcns.config.RadarPropertyHandler.Priority;
import org.radarcns.kafka.AggregateKey;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.empatica.EmpaticaE4Acceleration;
import org.radarcns.stream.SensorStreamWorker;
import org.radarcns.stream.StreamDefinition;
import org.radarcns.stream.aggregator.AggregateList;

/**
 * Definition of Kafka Stream for aggregating data collected by Empatica E4 Accelerometer sensor.
 */
public class E4AccelerationStream extends
        SensorStreamWorker<ObservationKey, EmpaticaE4Acceleration> {

    @Override
    protected void initialize() {
        defineWindowedSensorStream("android_empatica_e4_acceleration");
        config.setDefaultPriority(Priority.HIGH);
    }

    @Override
    protected KStream<AggregateKey, AggregateList> implementStream(
            StreamDefinition definition,
            @Nonnull KStream<ObservationKey, EmpaticaE4Acceleration> kstream) {
        return aggregateFields(definition, kstream, new String[] {"x", "y", "z"},
                EmpaticaE4Acceleration.getClassSchema());
    }
}
