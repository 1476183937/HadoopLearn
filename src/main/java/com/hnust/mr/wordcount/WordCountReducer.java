package com.hnust.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        //求和
        int sum = 0;
        //累加求和
        for (IntWritable value : values) {

            sum+= value.get();
        }
        v.set(sum);
        //写出
        context.write(key,v);

    }
}
