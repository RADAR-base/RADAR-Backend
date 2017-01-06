package org.radarcns.stream.collector;

import org.radarcns.aggregator.DoubleArrayAggregator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Java class to aggregate data using Kafka Streams. Double Array is the base unit
 */
public class DoubleArrayCollector {
    private DoubleValueCollector[] collectors;

    /**
     * @param value new sample that has to be analysed
     */
    public DoubleArrayCollector add(double[] value) {
        if (collectors == null) {
            collectors = new DoubleValueCollector[value.length];
            for (int i = 0; i < value.length; i++) {
                collectors[i] = new DoubleValueCollector();
            }
        }
        if (collectors.length != value.length) {
            throw new IllegalArgumentException(
                    "The length of current input differs from the length of the value used to "
                    + "instantiate this collector");
        }
        for (int i = 0; i < collectors.length; i++) {
            collectors[i].add(value[i]);
        }

        return this;
    }

    @Override
    public String toString() {
        return Arrays.toString(collectors);
    }

    /**
     * @return Avro equivalent class represented by org.radarcns.aggregator.DoubleArrayAggregator
     */
    public DoubleArrayAggregator convertInAvro() {
        int len = collectors.length;
        List<Double> min = new ArrayList<>(len);
        List<Double> max = new ArrayList<>(len);
        List<Double> sum = new ArrayList<>(len);
        List<Double> count = new ArrayList<>(len);
        List<Double> avg = new ArrayList<>(len);
        List<Double> iqr = new ArrayList<>(len);
        List<List<Double>> quartile = new ArrayList<>(len);

        for (DoubleValueCollector collector : collectors) {
            min.add(collector.getMin());
            max.add(collector.getMax());
            sum.add(collector.getSum());
            count.add(collector.getCount());
            avg.add(collector.getAvg());
            iqr.add(collector.getIqr());
            quartile.add(collector.getQuartile());
        }

        return new DoubleArrayAggregator(min, max, sum, count, avg, quartile, iqr);
    }
}