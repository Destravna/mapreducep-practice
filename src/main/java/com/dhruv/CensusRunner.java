package com.dhruv;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;

import com.dhruv.MostWorkingTop25.DecreasingComparator;
import com.dhruv.MostWorkingTop25.MostWorkingMapper;
import com.dhruv.MostWorkingTop25.MostWorkingReducer;

public class CensusRunner {
    public static void main( String[] args ) throws IOException{
        JobConf conf = new JobConf(CensusRunner.class);
        org.apache.hadoop.mapreduce.Job job = new org.apache.hadoop.mapreduce.Job();
        /*
         * 
         * 
         * AVERAGE WORKHOURS BY AGE
         * 
         */

        // conf.setJobName("averageWorkHoursByAge");

        // conf.setMapperClass(CensusMapper.class);
        // conf.setMapOutputKeyClass(LongWritable.class);
        // conf.setMapOutputValueClass(NumPair.class);

        // conf.setCombinerClass(CensusCombiner.class);
  
        // conf.setOutputKeyClass(LongWritable.class);
        // conf.setOutputValueClass(DoubleWritable.class);
        // conf.setReducerClass(CensusReducer.class);
       
        /*
         * 
         * 
         * TOP 25 MOST WORKING HOURS
         * 
         */
        
        job.setMapperClass(MostWorkingMapper);

        // conf.setMapperClass(MostWorkingMapper.class);
        // conf.setMapOutputKeyClass(IntWritable.class);
        // conf.setMapOutputValueClass(NullWritable.class);
        // conf.setOutputKeyComparatorClass(DecreasingComparator.class);

        // conf.setOutputKeyClass(IntWritable.class);
        // conf.setOutputValueClass(NullWritable.class);
        // conf.setReducerClass(MostWorkingReducer.class);

        // conf.setInputFormat(TextInputFormat.class);
        // conf.setOutputFormat(TextOutputFormat.class);
        // FileInputFormat.setInputPaths(conf, new Path(args[0]));
        // FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        // JobClient.runJob(conf);
    }
}
