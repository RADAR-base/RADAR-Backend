package org.radarcns.stream.collector;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.radarcns.aggregator.DoubleArrayAggegator;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Java class to aggregate data using Kafka Streams. Double Array is the base unit
 */
public class DoubleArrayCollector {
    private ArrayList<Double> min = new ArrayList<>();
    private ArrayList<Double> max = new ArrayList<>();
    private ArrayList<Double> sum = new ArrayList<>();
    private ArrayList<Double> count = new ArrayList<>();
    private ArrayList<Double> avg = new ArrayList<>();
    private ArrayList<List<Double>> quartile = new ArrayList<>();
    private ArrayList<Double> iqr = new ArrayList<>();

    private ArrayList<LinkedList<Double>> list = new ArrayList<>();

    private int size = 0;

    /**
     * @param value: new sample that has to be analysed
     */
    public DoubleArrayCollector add(Double[] value){

        if(size == 0){
            size = value.length;
        }
        else if(size != value.length){
            throw new IllegalArgumentException("The size of current input differs from the size of the value used to instantiate this collector");
        }

        updateMin(value);
        updateMax(value);

        updateAvg(value);

        updateQuartile(value);

        return this;
    }

    /**
     * @param value: new sample that update min values
     */
    private void updateMin(Double[] value){
        if(min.isEmpty()){
            for(int i=0; i<value.length; i++){
                min.add(i,value[i]);
            }
        }
        else{
            for(int i=0; i<value.length; i++){
                if(min.get(i) > value[i]) {
                    min.add(i,value[i]);
                }
            }
        }
    }

    /**
     * @param value: new sample that update max values
     */
    private void updateMax(Double[] value){
        if(max.isEmpty()){
            for(int i=0; i<value.length; i++){
                max.add(i,value[i]);
            }
        }
        else{
            for(int i=0; i<value.length; i++){
                if(max.get(i) < value[i]) {
                    max.set(i,value[i]);
                }
            }
        }
    }

    /**
     * @param value: new sample that update average values
     */
    private void updateAvg(Double[] value){
        if(count.isEmpty() && sum.isEmpty() && avg.isEmpty()){
            for(int i=0; i<value.length; i++){
                count.add(i,1d);
                sum.add(i,value[i]);
                avg.add(i,value[i]);
            }
        }
        else{
            for(int i=0; i<value.length; i++){
                count.set(i,count.get(i)+1d);
                sum.set(i,sum.get(i)+value[i]);

                double nextAvg = sum.get(i)/count.get(i);
                avg.set(i,nextAvg);
            }
        }
    }

    /**
     * @param value: new sample that update quartiles values
     */
    private void updateQuartile(Double[] value){
        if(list.isEmpty()){
            for(int i=0; i<value.length; i++){
                LinkedList<Double> componentList = new LinkedList<>();
                componentList.addLast(value[i]);

                list.add(i, componentList);
            }
        }
        else{
            for(int i=0; i<value.length; i++){
                list.get(i).addLast(value[i]);
            }
        }

        quartile = new ArrayList<>();
        iqr = new ArrayList<>();
        LinkedList<Double> componentList;
        double[] data;
        for(int i=0; i<value.length; i++){

            componentList = list.get(i);

            data = new double[componentList.size()];
            for(int j = 0; j < componentList.size(); j++) data[j] = componentList.get(j);

            DescriptiveStatistics ds = new DescriptiveStatistics(data);

            ArrayList<Double> componentQuartile = new ArrayList<>();
            componentQuartile.add(0,ds.getPercentile(25));
            componentQuartile.add(1,ds.getPercentile(50));
            componentQuartile.add(2,ds.getPercentile(75));

            quartile.add(i,componentQuartile);

            Double componentIqr = ds.getPercentile(75) - ds.getPercentile(25);

            iqr.add(i,componentIqr);
        }
    }

    @Override
    public String toString() {

        System.out.println(quartile.size());

        return "DoubleArrayCollector{" +
                "min=[" + min.stream().map(Object::toString).collect(Collectors.joining(" ,")) +
                "] , max=[" + max.stream().map(Object::toString).collect(Collectors.joining(" ,")) +
                "] , sum=[" + sum.stream().map(Object::toString).collect(Collectors.joining(" ,")) +
                "] , count=[" + count.stream().map(Object::toString).collect(Collectors.joining(" ,")) +
                "] , avg=[" + avg.stream().map(Object::toString).collect(Collectors.joining(" ,")) +
                "] , quartile=[" + quartile.stream().map(item -> "["+item.stream().map(Object::toString).collect(Collectors.joining(" ,"))+"]").collect(Collectors.joining(" ,")) +
                "] , iqr=[" + iqr.stream().map(Object::toString).collect(Collectors.joining(" ,")) +
                "] , list=[" + list.stream().map(item -> "["+item.stream().map(Object::toString).collect(Collectors.joining(" ,"))+"]").collect(Collectors.joining(" ,")) +
                "] , size=" + size +
                '}';
    }

    /**
     * @return the Avro equivalent class represented by org.radarcns.aggregator.DoubleArrayAggegator
     */
    public DoubleArrayAggegator convertInAvro(){
        return new DoubleArrayAggegator(min,max,sum,count,avg,quartile,iqr);
    }
}
