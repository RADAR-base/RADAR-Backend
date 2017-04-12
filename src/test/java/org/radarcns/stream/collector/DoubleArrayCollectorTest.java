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
public class DoubleArrayCollectorTest {

    private DoubleArrayCollector arrayCollector;

    @Before
    public void setUp() {
        this.arrayCollector = new DoubleArrayCollector();
    }

    @Test
    public void add() {
        double[] arrayvalues = {0.15d, 1.0d, 2.0d, 3.0d};
        this.arrayCollector.add(arrayvalues);

        assertEquals("[DoubleValueCollector{min=0.15, max=0.15, sum=0.15, count=1.0, avg=0.15, quartile=[0.15, 0.15, 0.15], iqr=0.0, history=[0.15]}, DoubleValueCollector{min=1.0, max=1.0, sum=1.0, count=1.0, avg=1.0, quartile=[1.0, 1.0, 1.0], iqr=0.0, history=[1.0]}, DoubleValueCollector{min=2.0, max=2.0, sum=2.0, count=1.0, avg=2.0, quartile=[2.0, 2.0, 2.0], iqr=0.0, history=[2.0]}, DoubleValueCollector{min=3.0, max=3.0, sum=3.0, count=1.0, avg=3.0, quartile=[3.0, 3.0, 3.0], iqr=0.0, history=[3.0]}]" , this.arrayCollector.toString());
    }
}
