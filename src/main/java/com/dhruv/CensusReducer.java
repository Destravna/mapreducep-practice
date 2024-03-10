package com.dhruv;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CensusReducer extends MapReduceBase implements Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {
    @Override
    public void reduce(LongWritable age, Iterator<DoubleWritable> workHours, OutputCollector<LongWritable, DoubleWritable> reducerOutput,
            Reporter arg3) throws IOException {
        Double sum = 0.0;
        int count = 1;
        while(workHours.hasNext()){
            count++;
            sum += workHours.next().get();
        }
        Double average = sum/count;
        DoubleWritable averagDoubleWritable = new DoubleWritable(average);
        
        reducerOutput.collect(age, averagDoubleWritable);
        
    }

}
