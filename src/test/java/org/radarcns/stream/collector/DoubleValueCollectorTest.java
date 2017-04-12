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

package org.radarcns.stream.collector;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

/**
 * Created by nivethika on 20-12-16.
 */
public class DoubleValueCollectorTest {

    private DoubleValueCollector valueCollector ;

    @Before
    public void setUp() {
        this.valueCollector = new DoubleValueCollector();
    }

    @Test
    public void add() {
        valueCollector.add(10.0d);
        assertEquals(10.0d, valueCollector.getMin(), 0.0d);
        assertEquals(10.0d, valueCollector.getMax(), 0.0d);
        assertEquals(10.0d, valueCollector.getSum(), 0.0d);
        assertEquals(10.0d, valueCollector.getAvg(), 0.0d);
        assertEquals(0.0d, valueCollector.getIqr(), 0.0d);
        assertEquals(1, valueCollector.getCount(),0);

        valueCollector.add(15.100d);
        assertEquals(10.0d, valueCollector.getMin(), 0.0d);
        assertEquals(15.100d, valueCollector.getMax(), 0.0d);
        assertEquals(25.100d, valueCollector.getSum(), 0.0d);
        assertEquals(12.550d, valueCollector.getAvg(), 0.0d);
        assertEquals(5.1, valueCollector.getIqr(), 0.0d);
        assertEquals(2, valueCollector.getCount(),0);

        valueCollector.add(28.100d);
        assertEquals(18.1d, valueCollector.getIqr(), 0.0d);

    }

    @Test
    public void addFloat() {
        valueCollector.add(10.0234f);
        assertEquals(10.0234d, valueCollector.getMin(), 0.0d);
        assertEquals(10.0234d, valueCollector.getMax(), 0.0d);
        assertEquals(10.0234d, valueCollector.getSum(), 0.0d);
        assertEquals(10.0234d, valueCollector.getAvg(), 0.0d);
        assertEquals(0.0d, valueCollector.getIqr(), 0.0d);
        assertEquals(1, valueCollector.getCount(),0);

        valueCollector.add(15.0d);
        assertEquals(10.0234d, valueCollector.getMin(), 0.0d);
        assertEquals(15.0d, valueCollector.getMax(), 0.0d);
        assertEquals(25.0234d, valueCollector.getSum(), 0.0d);
        assertEquals(12.5117d, valueCollector.getAvg(), 0.0d);
        assertEquals(4.9766d, valueCollector.getIqr(), 0.0d);
        assertEquals(2, valueCollector.getCount(),0);

        valueCollector.add(28.100d);
        assertEquals(18.0766d, valueCollector.getIqr(), 0.0d);

    }

    @Test
    public void testAverage() {
        double[] input = {36.793899922141186, 36.878288191353626, 36.965575690177715, 36.988087035729855, 36.628622572158214};
        for (double d : input) {
            valueCollector.add(d);
        }
        assertEquals(36.850894682312116, valueCollector.getAvg(), Math.pow(10, -10.0));
    }
}
