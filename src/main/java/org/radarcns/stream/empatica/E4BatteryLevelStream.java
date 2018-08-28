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
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.kafka.AggregateKey;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.empatica.EmpaticaE4BatteryLevel;
import org.radarcns.stream.KStreamWorker;
import org.radarcns.stream.StreamDefinition;
import org.radarcns.stream.StreamMaster;
import org.radarcns.stream.aggregator.NumericAggregate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Stream for aggregating data about Empatica E4 battery level.
 */
public class E4BatteryLevelStream extends KStreamWorker<ObservationKey, EmpaticaE4BatteryLevel> {
    private static final Logger logger = LoggerFactory.getLogger(E4BatteryLevelStream.class);

    public E4BatteryLevelStream(int numThread, StreamMaster master,
            RadarPropertyHandler properties) {
        super(numThread, master, properties, logger);
        createWindowedSensorStream("android_empatica_e4_battery_level");
    }

    @Override
    protected KStream<AggregateKey, NumericAggregate> implementStream(StreamDefinition definition,
            @Nonnull KStream<ObservationKey, EmpaticaE4BatteryLevel> kstream) {
        return aggregateNumeric(definition, kstream, "batteryLevel",
                EmpaticaE4BatteryLevel.getClassSchema());
    }
}
