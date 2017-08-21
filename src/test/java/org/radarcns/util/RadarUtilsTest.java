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

import static org.junit.Assert.assertEquals;

import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.junit.Before;
import org.junit.Test;
import org.radarcns.empatica.EmpaticaE4Acceleration;
import org.radarcns.key.MeasurementKey;
import org.radarcns.key.WindowedKey;

public class RadarUtilsTest {

    private RadarUtilities radarUtilities;

    @Before
    public void setUp() {
        this.radarUtilities = new RadarUtilitiesImpl();
    }

    @Test
    public void getWindowed() {
        String userId = "userId";
        MeasurementKey measurementKey = new MeasurementKey();
        measurementKey.setUserId(userId);
        Window window = new TimeWindow(1, 4);
        Windowed<MeasurementKey> measurementKeyWindowed = new Windowed<>(measurementKey, window);

        WindowedKey windowedKey = radarUtilities.getWindowed(measurementKeyWindowed);
        assertEquals(windowedKey.getUserID(), userId);
    }

    @Test
    public void ibiToHR() {
        double hR = radarUtilities.ibiToHeartRate(1.0f);
        assertEquals((60d / 1.0d), hR, 0.0d);
    }

    @Test
    public void accelerationToArray() {
        EmpaticaE4Acceleration acceleration = new EmpaticaE4Acceleration();
        acceleration.setX(1.0f);
        acceleration.setY(2.0f);
        acceleration.setZ(3.0f);

        double[] value = radarUtilities.accelerationToArray(acceleration);
        assertEquals(value[0], 1.0f, 0.0);
        assertEquals(value[1], 2.0f, 0.0);
        assertEquals(value[2], 3.0f, 0.0);
    }

}
