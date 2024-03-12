package com.dhruv.MostWorkingTop25;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MostWorkingMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, NullWritable> {
    @Override
    public void map(LongWritable arg0, Text line, OutputCollector<IntWritable, NullWritable> outputCollector, Reporter arg3)
            throws IOException {
        final String lineString = line.toString();
        StringTokenizer tokens = new StringTokenizer(lineString, ", ");
        int count = 0, workHours = 0;
        while(tokens.hasMoreTokens()){
            if(count == 12) {
                String workHoursString = tokens.nextToken().toString();
                workHours = Integer.parseInt(workHoursString);
                break;
            }
            tokens.nextToken();
            count++;
        }
        outputCollector.collect(new IntWritable(workHours), NullWritable.get());
        

        
    }
    
}
