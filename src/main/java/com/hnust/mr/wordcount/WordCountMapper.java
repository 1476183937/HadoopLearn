package com.hnust.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//LongWritable:输入数据key的类型
//Text：输入数据value的类型
//Text：输出数据key的类型
//IntWritable：输出数据value的类型
public class WordCountMapper extends Mapper<LongWritable,Text,Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable(1);  //因为是设置输出value为1，所以直接初始化为1
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取的一行输入：atguigu atguigu
        //获取一行
        String line = value.toString();
        //切割单词
        String[] words = line.split(" ");
        //循环遍历
        for (String word : words) {

            //将单词(atguigu)作为key
            k.set(word);
            //设置value为1：IntWritable v = new IntWritable(1);
            context.write(k,v);
        }

    }
}
