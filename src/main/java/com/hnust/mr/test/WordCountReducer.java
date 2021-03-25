package com.hnust.mr.test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Title:
 * @Author: ggh
 * @Date: 2020/11/26 22:41
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable value : values) {

            sum += value.get();

        }

        context.write(key,new IntWritable(sum));

    }
}
