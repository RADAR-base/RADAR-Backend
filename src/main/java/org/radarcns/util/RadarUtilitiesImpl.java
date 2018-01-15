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

import static org.apache.kafka.streams.KeyValue.pair;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Window;
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
 * Implements {@link RadarUtilities}.
 */
public class RadarUtilitiesImpl implements RadarUtilities {
    protected RadarUtilitiesImpl() {
        // used for construction from RadarSingletonFactory
    }

    @Override
    public AggregateKey getWindowed(Windowed<ObservationKey> window) {
        ObservationKey key = window.key();
        Window timeWindow = window.window();
        return new AggregateKey(key.getProjectId(), key.getUserId(), key.getSourceId(),
                timeWindow.start() / 1000d, timeWindow.end() / 1000d);
    }

    @Override
    public AggregateKey getWindowedTuple(Windowed<TemporaryPackageKey> window) {
        TemporaryPackageKey key = window.key();
        Window timeWindow = window.window();
        return new AggregateKey(key.getProjectId(), key.getUserId(), key.getSourceId(),
                timeWindow.start() / 1000d, timeWindow.end() / 1000d);
    }

    @Override
    public KeyValue<AggregateKey, PhoneUsageAggregate> phoneCollectorToAvro(
            Windowed<TemporaryPackageKey> window, PhoneUsageCollector collector
    ) {
        return pair(getWindowedTuple(window) , new PhoneUsageAggregate(
                window.key().getPackageName(),
                collector.getTotalForegroundTime(),
                collector.getTimesTurnedOn(),
                collector.getCategoryName(),
                collector.getCategoryNameFetchTime()
        ));
    }

    @Override
    public KeyValue<AggregateKey, AggregateList> listCollectorToAvro(Windowed<ObservationKey> window, AggregateListCollector collector) {
        List<NumericAggregate> fields = collector.getCollectors().stream()
                .map(this::numericCollectorToAggregate)
                .collect(Collectors.toList());

        return pair(getWindowed(window), new AggregateList(fields));
    }

    @Override
    public KeyValue<AggregateKey, NumericAggregate> numericCollectorToAvro(
            Windowed<ObservationKey> window, NumericAggregateCollector collector) {
        return pair(getWindowed(window), numericCollectorToAggregate(collector));
    }

    private NumericAggregate numericCollectorToAggregate(NumericAggregateCollector collector) {
        return new NumericAggregate(collector.getName(), collector.getMin(), collector.getMax(),
                collector.getSum(), collector.getCount(), collector.getMean(),
                collector.getQuartile());
    }
}
