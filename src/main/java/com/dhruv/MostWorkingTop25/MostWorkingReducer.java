package com.dhruv.MostWorkingTop25;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MostWorkingReducer extends MapReduceBase implements Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {

    private static int size = 25;


    @Override
    public void configure(JobConf job) {
        // priorityQueue = new PriorityQueue<>(Collections.reverseOrder());
    }

    @Override
    public void reduce(IntWritable workHour, Iterator<NullWritable> arg1, OutputCollector<IntWritable, NullWritable> outputCollector,
            Reporter arg3) throws IOException {
        System.out.println("I am a useless reducer");
        // priorityQueue.add(workHour.get());
        if(size > 0){
            outputCollector.collect(workHour, null);
            size--;
        }
        close();   
    }

    @Override
    public void close() throws IOException {
        
    }
}