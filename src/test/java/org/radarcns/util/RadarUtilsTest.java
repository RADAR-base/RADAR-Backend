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
import org.radarcns.kafka.AggregateKey;
import org.radarcns.kafka.ObservationKey;

public class RadarUtilsTest {

    private RadarUtilities radarUtilities;

    @Before
    public void setUp() {
        this.radarUtilities = new RadarUtilitiesImpl();
    }

    @Test
    public void getWindowed() {
        String userId = "userId";
        String sourceId = "sourceId";

        ObservationKey measurementKey = new ObservationKey();
        measurementKey.setUserId(userId);
        measurementKey.setSourceId(sourceId);

        Window window = new TimeWindow(1, 4);
        Windowed<ObservationKey> measurementKeyWindowed = new Windowed<>(measurementKey, window);

        AggregateKey windowedKey = radarUtilities.getWindowed(measurementKeyWindowed);

        assertEquals(windowedKey.getUserId(), userId);
        assertEquals(windowedKey.getSourceId(), sourceId);
    }
}
