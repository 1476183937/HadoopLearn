package com.hnust.mr.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TwoIndexMapper extends Mapper<LongWritable, Text,Text,Text> {

    Text k = new Text();
    Text v = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //输入
        //atguigu--a.txt	3
        //输出
        //atguigu a.txt 3

        String line = value.toString();

        //用“--”切割
        String[] fileds = line.split("--");

        k.set(fileds[0]);
        v.set(fileds[1]);
        context.write(k,v);
    }
}
