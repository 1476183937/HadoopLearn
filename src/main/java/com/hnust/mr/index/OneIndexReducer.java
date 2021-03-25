package com.hnust.mr.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OneIndexReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        //输入
//        atguigu--a.txt
//        atguigu--b.txt
//        atguigu--c.txt
        //输出
        //atguigu--a.txt 2
        // atguigu--b.txt 3

        int sum = 0;

        for (IntWritable value : values) {

            sum += value.get();
        }
        k.set(key);
        v.set(sum);
        context.write(k,v);


    }
}
