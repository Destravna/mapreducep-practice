package com.dhruv;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CensusReducer extends MapReduceBase implements Reducer<LongWritable, NumPair, LongWritable, DoubleWritable> {
    @Override
    public void reduce(LongWritable age, Iterator<NumPair> workHourFrequencyPair, OutputCollector<LongWritable, DoubleWritable> reducerOutput,
            Reporter arg3) throws IOException {
        Double sum = 0.0;
        int count = 0;
        while(workHourFrequencyPair.hasNext()){
            NumPair numPair = workHourFrequencyPair.next();
            count += numPair.getSecond().get();
           sum += numPair.getFirst().get();
        }
        Double average = sum/count;
        DoubleWritable averagDoubleWritable = new DoubleWritable(average);
        
        reducerOutput.collect(age, averagDoubleWritable);
        
    }

}
