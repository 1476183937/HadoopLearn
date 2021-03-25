package com.hnust.mr.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountSortMapper extends Mapper<LongWritable,Text,FlowBean, Text> {

    private FlowBean k = new FlowBean();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //读取一行
        String line = value.toString();

        //获取手机号码，上行流量，下行流量
        String[] split = line.split("\t");
        String phoneNum = split[0];
        long upFlow = Long.parseLong(split[1]);
        long downFlow = Long.parseLong(split[2]);

        //封装Flowbean
        k.set(upFlow,downFlow);
        v.set(phoneNum);
        context.write(k,v);
    }
}
