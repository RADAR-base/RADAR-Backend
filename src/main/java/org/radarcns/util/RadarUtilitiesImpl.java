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

import org.apache.kafka.streams.kstream.Windowed;
import org.radarcns.empatica.EmpaticaE4Acceleration;
import org.radarcns.key.MeasurementKey;
import org.radarcns.key.WindowedKey;

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

    public double floatToDouble(float input) {
        return Double.parseDouble(String.valueOf(input));
    }

    public double ibiToHeartRate(float input) {
        return 60d / floatToDouble(input);
    }

    public double[] accelerationToArray(EmpaticaE4Acceleration value) {
        return new double[] {
                floatToDouble(value.getX()),
                floatToDouble(value.getY()),
                floatToDouble(value.getZ())};
    }

}
