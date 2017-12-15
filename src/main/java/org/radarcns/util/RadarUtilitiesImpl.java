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
import org.radarcns.passive.empatica.EmpaticaE4Acceleration;
import org.radarcns.stream.aggregator.DoubleAggregation;
import org.radarcns.stream.aggregator.DoubleArrayAggregation;
import org.radarcns.stream.aggregator.PhoneUsageAggregation;
import org.radarcns.stream.collector.DoubleArrayCollector;
import org.radarcns.stream.collector.DoubleValueCollector;
import org.radarcns.stream.phone.PhoneUsageCollector;
import org.radarcns.stream.phone.TemporaryPackageKey;

import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.streams.KeyValue.pair;

/**
 * Implements {@link RadarUtilities}.
 */
public class RadarUtilitiesImpl implements RadarUtilities {
    protected RadarUtilitiesImpl() {
        // used for construction from RadarSingletonFactory
    }

    @Override
    public AggregateKey getWindowed(Windowed<ObservationKey> window) {
        return new AggregateKey(window.key().getProjectId(), window.key().getUserId(),
                window.key().getSourceId(), window.window().start(), window.window().end());
    }

    @Override
    public AggregateKey getWindowedTuple(Windowed<TemporaryPackageKey> window) {
        TemporaryPackageKey temp = window.key();
        ObservationKey measurementKey = new ObservationKey(temp.getProjectId(), temp.getUserId(),
                temp.getSourceId());
        return new AggregateKey(measurementKey.getProjectId(), measurementKey.getUserId(),
                measurementKey.getSourceId(), window.window().start(), window.window().end());
    }

    @Override
    public double floatToDouble(float input) {
        return Double.parseDouble(String.valueOf(input));
    }


    @Override
    public KeyValue<AggregateKey, PhoneUsageAggregation> collectorToAvro(
            Windowed<TemporaryPackageKey> window, PhoneUsageCollector collector
    ) {
        return pair(getWindowedTuple(window) , new PhoneUsageAggregation(
                window.key().getPackageName(),
                collector.getTotalForegroundTime(),
                collector.getTimesTurnedOn(),
                collector.getCategoryName(),
                collector.getCategoryNameFetchTime()
        ));
    }

    @Override
    public KeyValue<AggregateKey, DoubleArrayAggregation> collectorToAvro(
            Windowed<ObservationKey> window, DoubleArrayCollector collector) {
        List<DoubleValueCollector> subcollectors = collector.getCollectors();
        int len = subcollectors.size();
        List<Double> min = new ArrayList<>(len);
        List<Double> max = new ArrayList<>(len);
        List<Double> sum = new ArrayList<>(len);
        List<Double> count = new ArrayList<>(len);
        List<Double> avg = new ArrayList<>(len);
        List<Double> iqr = new ArrayList<>(len);
        List<List<Double>> quartile = new ArrayList<>(len);

        for (DoubleValueCollector subcollector : subcollectors) {
            min.add(subcollector.getMin());
            max.add(subcollector.getMax());
            sum.add(subcollector.getSum());
            count.add(subcollector.getCount());
            avg.add(subcollector.getAvg());
            iqr.add(subcollector.getIqr());
            quartile.add(subcollector.getQuartile());
        }

        return pair(getWindowed(window),
                new DoubleArrayAggregation(min, max, sum, count, avg, quartile, iqr));
    }

    @Override
    public KeyValue<AggregateKey, DoubleAggregation> collectorToAvro(
            Windowed<ObservationKey> window, DoubleValueCollector collector) {
        return pair(getWindowed(window),
                new DoubleAggregation(collector.getMin(), collector.getMax(), collector.getSum(),
                        collector.getCount(), collector.getAvg(), collector.getQuartile(),
                        collector.getIqr()));
    }

    @Override
    public double ibiToHeartRate(float input) {
        return 60d / floatToDouble(input);
    }

    @Override
    public double[] accelerationToArray(EmpaticaE4Acceleration value) {
        return new double[] {
                floatToDouble(value.getX()),
                floatToDouble(value.getY()),
                floatToDouble(value.getZ())};
    }

}
