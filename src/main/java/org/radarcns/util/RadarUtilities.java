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

package org.radarcns.util;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.radarcns.kafka.AggregateKey;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.stream.aggregator.AggregateList;
import org.radarcns.stream.aggregator.NumericAggregate;
import org.radarcns.stream.aggregator.PhoneUsageAggregate;
import org.radarcns.stream.collector.AggregateListCollector;
import org.radarcns.stream.collector.NumericAggregateCollector;
import org.radarcns.stream.phone.PhoneUsageCollector;
import org.radarcns.stream.phone.TemporaryPackageKey;

/**
 * Interface that facades all utility functions that are required to support RadarBackend features.
 */
public interface RadarUtilities {

    /**
     * Creates a AggregateKey for a window of ObservationKey.
     * @param window Windowed measurement keys
     * @return relevant AggregateKey
     */
    AggregateKey getWindowed(Windowed<ObservationKey> window);

    AggregateKey getWindowedTuple(Windowed<TemporaryPackageKey> window);

    KeyValue<AggregateKey, AggregateList> listCollectorToAvro(
            Windowed<ObservationKey> window, AggregateListCollector collector);

    KeyValue<AggregateKey, NumericAggregate> numericCollectorToAvro(
            Windowed<ObservationKey> window, NumericAggregateCollector collector);

    KeyValue<AggregateKey, PhoneUsageAggregate> phoneCollectorToAvro(
            Windowed<TemporaryPackageKey> window, PhoneUsageCollector collector);
}
