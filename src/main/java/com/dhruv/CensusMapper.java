package com.dhruv;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

// average workhours by age
public class CensusMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, NumPair> {
    @Override
    public void map(LongWritable lineNumber, Text recordLine, OutputCollector<LongWritable, NumPair> outputPair, Reporter arg3)
            throws IOException {
        
        int tokenIndex = 0, workHours = 0, age = 0;
        String line = recordLine.toString();
        StringTokenizer tokenizer = new StringTokenizer(line, ", ");
        
        while(tokenizer.hasMoreTokens()){
            String token = tokenizer.nextToken().toString();
            if(tokenIndex == 0) age = Integer.parseInt(token);
            if(tokenIndex == 12) workHours = Integer.parseInt(token);
            tokenIndex++;
        }
        LongWritable ageKey = new LongWritable(age);
        DoubleWritable workHoursValue = new DoubleWritable((double)workHours);
        outputPair.collect(ageKey, new NumPair(workHoursValue, new IntWritable(1)));
        
    }
}
