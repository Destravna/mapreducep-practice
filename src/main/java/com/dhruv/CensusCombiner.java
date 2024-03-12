package com.dhruv;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CensusCombiner extends MapReduceBase implements Reducer<LongWritable, NumPair, LongWritable, NumPair> {

    @Override
    public void reduce(LongWritable age, Iterator<NumPair> workHourFrequencyPairWithCount1, OutputCollector<LongWritable, NumPair> outputCollector, Reporter arg3) throws IOException {
        int count = 0;
        double workHours = 0;
        while(workHourFrequencyPairWithCount1.hasNext()){
            NumPair numPair = workHourFrequencyPairWithCount1.next();
            count += numPair.getSecond().get();
            workHours += numPair.getFirst().get();
        }
        outputCollector.collect(age, new NumPair(new DoubleWritable(workHours), new IntWritable(count)));
    }

}
