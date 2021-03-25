package com.hnust.mr.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderSortMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {

    OrderBean k = new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

//        0000001	Pdt_01	222.8
        //读取一行
        String line = value.toString();
        String[] fileds = line.split("\t");
        //封装OederBean
        k.setOrder_id(Integer.parseInt(fileds[0]));
        k.setPrice(Double.parseDouble(fileds[2]));
        context.write(k,NullWritable.get());


    }
}
