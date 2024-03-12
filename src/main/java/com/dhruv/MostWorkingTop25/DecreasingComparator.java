package com.dhruv.MostWorkingTop25;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DecreasingComparator extends WritableComparator {
    protected DecreasingComparator() {
        super(IntWritable.class, true);
    }

    public int compare(Writable a, Writable b) {
        IntWritable aIntWritable = (IntWritable)a;
        IntWritable bIntWritable = (IntWritable)b;
        return -1 * aIntWritable.compareTo(bIntWritable);
    }
}
