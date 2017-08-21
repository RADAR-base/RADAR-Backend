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
import org.radarcns.aggregator.DoubleAggregator;
import org.radarcns.aggregator.DoubleArrayAggregator;
import org.radarcns.empatica.EmpaticaE4Acceleration;
import org.radarcns.key.MeasurementKey;
import org.radarcns.key.WindowedKey;
import org.radarcns.stream.collector.DoubleArrayCollector;
import org.radarcns.stream.collector.DoubleValueCollector;

import java.util.ArrayList;
import java.util.List;

/**
 * Implements {@link RadarUtilities}
 */
public class RadarUtilitiesImpl implements RadarUtilities {
    protected RadarUtilitiesImpl() {
        // used for construction from RadarSingletonFactory
    }

    public WindowedKey getWindowed(Windowed<MeasurementKey> window) {
        return new WindowedKey(window.key().getUserId(), window.key().getSourceId(),
                window.window().start(), window.window().end());
    }

    @Override
    public KeyValue<WindowedKey, DoubleArrayAggregator> collectorToAvro(
            Windowed<MeasurementKey> window, DoubleArrayCollector collector) {
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

        return new KeyValue<>(getWindowed(window),
                new DoubleArrayAggregator(min, max, sum, count, avg, quartile, iqr));
    }

    @Override
    public KeyValue<WindowedKey, DoubleAggregator> collectorToAvro(
            Windowed<MeasurementKey> window, DoubleValueCollector collector) {
        return new KeyValue<>(getWindowed(window),
                new DoubleAggregator(collector.getMin(), collector.getMax(), collector.getSum(),
                        collector.getCount(), collector.getAvg(), collector.getQuartile(),
                        collector.getIqr()));
    }

    public double ibiToHeartRate(float input) {
        return 60d / input;
    }

    public double[] accelerationToArray(EmpaticaE4Acceleration value) {
        return new double[]{value.getX(), value.getY(), value.getZ()};
    }

}
