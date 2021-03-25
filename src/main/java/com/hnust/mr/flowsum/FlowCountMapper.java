package com.hnust.mr.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text,Text,FlowBean> {

    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //        1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
        //读取一行
        String line = value.toString();
        //切割
        String[] values = line.split("\t");
        //取出电话号码和上行、下行流量
        String phoneNum = values[1];
        String upFlow = values[values.length - 3];
        String downFlow = values[values.length - 2];

        k.set(phoneNum);
        v.set(Long.parseLong(upFlow),Long.parseLong(downFlow));

        //写出
        context.write(k,v);
    }
}
