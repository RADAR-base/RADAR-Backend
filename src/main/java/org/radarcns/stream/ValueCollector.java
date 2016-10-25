package org.radarcns.stream;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.radarcns.Statistic;

import java.util.Arrays;
import java.util.LinkedList;

/**
 * Created by Francesco Nobilia on 21/10/2016.
 */
public class ValueCollector {
    private double min = Double.MAX_VALUE;
    private double max = Double.MIN_VALUE;
    private double sum = 0;
    private double count = 0;
    private double avg = 0;
    private Double quartile[] = new Double[3];
    private double iqr = 0;

    private LinkedList<Double> list = new LinkedList<>();

    public ValueCollector add(Double value){

        updateMin(value);
        updateMax(value);

        updateAvg(value);

        updateQuartile(value);

        return this;
    }

    private void updateMin(double value){
        if(min > value){
            min = value;
        }
    }

    private void updateMax(double value){
        if(max < value){
            max = value;
        }
    }

    private void updateAvg(double value){
        count++;
        sum += value;

        avg = sum / count;
    }

    private void updateQuartile(double value){
        list.addLast(value);

        double[] data = new double[list.size()];
        for(int i = 0; i < list.size(); i++) data[i] = list.get(i);

        DescriptiveStatistics ds = new DescriptiveStatistics(data);

        quartile[0] = ds.getPercentile(25);
        quartile[1] = ds.getPercentile(50);
        quartile[2] = ds.getPercentile(75);

        iqr = ds.getPercentile(75) - ds.getPercentile(25);
    }

    @Override
    public String toString() {
        return "ValueCollector{" +
                "min=" + min +
                ", max=" + max +
                ", sum=" + sum +
                ", count=" + count +
                ", avg=" + avg +
                ", quartile=" + Arrays.toString(quartile) +
                ", iqr=" + iqr +
                ", list=" + list +
                '}';
    }

    public Statistic convertInAvro(){
        return new Statistic(min,max,sum,count,avg,Arrays.asList(quartile),iqr);
    }
}
