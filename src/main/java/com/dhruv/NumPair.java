package com.dhruv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class NumPair implements WritableComparable<NumPair> {
    DoubleWritable first;
    IntWritable second;

    public NumPair() {

    }

    // public NumPair(Double first, Integer second) {
    //     this.first = new DoubleWritable(first);
    //     this.second = new IntWritable(second);
    // }

    public NumPair(DoubleWritable first, IntWritable second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        if (first == null) {
            first = new DoubleWritable();
        }
        if (second == null) {
            second = new IntWritable();
        }
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public int compareTo(NumPair o) {
        return first.compareTo(o.getFirst());
    }

    public DoubleWritable getFirst() {
        return first;
    }

    public void setFirst(DoubleWritable first) {
        this.first = first;
    }

    public IntWritable getSecond() {
        return second;
    }

    public void setSecond(IntWritable second) {
        this.second = second;
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof NumPair) {
            NumPair o = (NumPair) obj;
            return first.compareTo(o.getFirst()) == 0 && second.compareTo(o.getSecond()) == 0;
        }
        return false;
    }

}
