package org.radarcns.stream.collector;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
        assertEquals(25.023400000000002d, valueCollector.getSum(), 0.0d);
        assertEquals(12.511700000000001d, valueCollector.getAvg(), 0.0d);
        assertEquals(15.0d-10.0234d, valueCollector.getIqr(), 0.0d);
        assertEquals(2, valueCollector.getCount(),0);

        valueCollector.add(28.100d);
        assertEquals(18.0766d, valueCollector.getIqr(), 0.0d);

    }
}
