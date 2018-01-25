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

package org.radarcns.util.serde;

import org.apache.kafka.common.serialization.Serde;
import org.radarcns.stream.collector.AggregateListCollector;
import org.radarcns.stream.collector.NumericAggregateCollector;
import org.radarcns.stream.phone.PhoneUsageCollector;

/**
 * Set of Serde useful for Kafka Streams
 */
public final class RadarSerdes {
    private final Serde<NumericAggregateCollector> numericCollector;
    private final Serde<AggregateListCollector> aggregateListCollector;
    private final Serde<PhoneUsageCollector> phoneUsageCollector;

    private static RadarSerdes instance = new RadarSerdes();

    public static RadarSerdes getInstance() {
        return instance;
    }

    private RadarSerdes() {
        numericCollector = new RadarSerde<>(NumericAggregateCollector.class).getSerde();
        aggregateListCollector = new RadarSerde<>(AggregateListCollector.class).getSerde();
        phoneUsageCollector = new RadarSerde<>(PhoneUsageCollector.class).getSerde();
    }

    public Serde<NumericAggregateCollector> getNumericAggregateCollector() {
        return numericCollector;
    }

    public Serde<AggregateListCollector> getAggregateListCollector()  {
        return aggregateListCollector;
    }

    public Serde<PhoneUsageCollector> getPhoneUsageCollector() {
        return phoneUsageCollector;
    }
}
