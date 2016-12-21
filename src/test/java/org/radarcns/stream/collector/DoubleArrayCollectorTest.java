package org.radarcns.stream.collector;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by nivethika on 20-12-16.
 */
public class DoubleArrayCollectorTest {

    private DoubleArrayCollector arrayCollector;
    private int numOfElem =4;

    @Before
    public void setUp() {
        this.arrayCollector = new DoubleArrayCollector(this.numOfElem);
    }

    @Test
    public void add() {
        double[] arrayvalues = {0.15d, 1.0d, 2.0d, 3.0d};
        this.arrayCollector.add(arrayvalues);

        assertEquals("[DoubleValueCollector{min=0.15, max=0.15, sum=0.15, count=1.0, avg=0.15, quartile=[0.15, 0.15, 0.15], iqr=0.0, list=[0.15]}, DoubleValueCollector{min=1.0, max=1.0, sum=1.0, count=1.0, avg=1.0, quartile=[1.0, 1.0, 1.0], iqr=0.0, list=[1.0]}, DoubleValueCollector{min=2.0, max=2.0, sum=2.0, count=1.0, avg=2.0, quartile=[2.0, 2.0, 2.0], iqr=0.0, list=[2.0]}, DoubleValueCollector{min=3.0, max=3.0, sum=3.0, count=1.0, avg=3.0, quartile=[3.0, 3.0, 3.0], iqr=0.0, list=[3.0]}]" , this.arrayCollector.toString());

    }
}
