package com.hnust.mr.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OneIndexMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    String name;
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取文件名
        FileSplit split = (FileSplit) context.getInputSplit();
        name = split.getPath().getName();

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //输入
//        atguigu pingping
//        atguigu ss
//        atguigu ss
        //输出
        //atguigu--a.txt
        //atguigu--b.txt

        String line = value.toString();

        String[] fileds = line.split(" ");

        for (String filed : fileds) {
            //拼接
            filed = filed + "--" +name;
            k.set(filed);
            context.write(k,v);
        }

    }
}
