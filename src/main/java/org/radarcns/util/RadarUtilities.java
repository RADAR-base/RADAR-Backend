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

/**
 * Interface that facades all utility functions that are required to support RadarBackend features.
 */
public interface RadarUtilities {

    /**
     * Creates a WindowedKey for a window of MeasurementKey.
     * @param window Windowed measurement keys
     * @return relevant WindowedKey
     */
    WindowedKey getWindowed(Windowed<MeasurementKey> window);

    KeyValue<WindowedKey, DoubleArrayAggregator> collectorToAvro(
            Windowed<MeasurementKey> window, DoubleArrayCollector collector);

    KeyValue<WindowedKey, DoubleAggregator> collectorToAvro(
            Windowed<MeasurementKey> window, DoubleValueCollector collector);

    double ibiToHeartRate(float input);

    double[] accelerationToArray(EmpaticaE4Acceleration value);
}
