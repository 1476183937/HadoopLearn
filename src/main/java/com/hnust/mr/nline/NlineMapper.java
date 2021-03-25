package com.hnust.mr.nline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NlineMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    IntWritable v = new IntWritable(1);
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] split = line.split(" ");
        for (String s : split) {
            k.set(s);
            context.write(k,v);
        }

    }
}
