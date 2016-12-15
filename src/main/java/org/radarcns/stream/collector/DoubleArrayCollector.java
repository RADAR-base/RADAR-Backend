package org.radarcns.stream.collector;

import org.radarcns.aggregator.DoubleArrayAggegator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.radarcns.util.RadarUtils.floatToDouble;

/**
 * Java class to aggregate data using Kafka Streams. Double Array is the base unit
 */
public class DoubleArrayCollector {
    private final DoubleValueCollector[] aggregators;

    public DoubleArrayCollector(int length) {
        aggregators = new DoubleValueCollector[length];
    }

    /**
     * @param value: new sample that has to be analysed
     */
    public DoubleArrayCollector add(double[] value) {
        if (aggregators.length != value.length) {
            throw new IllegalArgumentException(
                    "The length of current input differs from the length of the value used to instantiate this collector");
        }
        for (int i = 0; i < aggregators.length; i++) {
            aggregators[i].add(value[i]);
        }

        return this;
    }

    @Override
    public String toString() {
        return Arrays.asList(aggregators).toString();
    }

    /**
     * @return the Avro equivalent class represented by org.radarcns.aggregator.DoubleArrayAggegator
     */
    public DoubleArrayAggegator convertInAvro() {
        int len = aggregators.length;
        List<Double> min = new ArrayList<>(len), max = new ArrayList<>(len), sum = new ArrayList<>(len), count = new ArrayList<>(len), avg = new ArrayList<>(len), iqr = new ArrayList<>(len);
        List<List<Double>> quartile = new ArrayList<>(len);

        for (DoubleValueCollector aggregator : aggregators) {
            min.add(aggregator.getMin());
            max.add(aggregator.getMax());
            sum.add(aggregator.getSum());
            count.add(aggregator.getCount());
            avg.add(aggregator.getAvg());
            iqr.add(aggregator.getIqr());
            quartile.add(aggregator.getQuartile());
        }

        return new DoubleArrayAggegator(min, max, sum, count, avg, quartile, iqr);
    }
}