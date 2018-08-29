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
import org.radarcns.passive.empatica.EmpaticaE4Temperature;
import org.radarcns.stream.SensorStreamWorker;
import org.radarcns.stream.StreamDefinition;
import org.radarcns.stream.aggregator.NumericAggregate;

/**
 * Definition of Kafka Stream for aggregating temperature values collected by Empatica E4.
 */
public class E4TemperatureStream extends SensorStreamWorker<ObservationKey, EmpaticaE4Temperature> {
    @Override
    protected void initialize() {
        defineWindowedSensorStream("android_empatica_e4_temperature");
        config.setDefaultPriority(Priority.NORMAL);
    }

    @Override
    protected KStream<AggregateKey, NumericAggregate> implementStream(StreamDefinition definition,
            @Nonnull KStream<ObservationKey, EmpaticaE4Temperature> kstream) {
        return aggregateNumeric(definition, kstream, "temperature",
                EmpaticaE4Temperature.getClassSchema());
    }
}
