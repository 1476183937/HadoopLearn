package com.hnust.mr.table;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable,Text, Text,TableBean> {

    private String name;
    private Text k = new Text();
    TableBean tableBean = new TableBean();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取切片
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        //通过切片获取文件名
        name = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();


        if (name.startsWith("order")){//来自订单表

            String[] fileds = line.split("\t");
            tableBean.setOrder_id(fileds[0]);
            tableBean.setP_id(fileds[1]);
            tableBean.setAmount(Integer.parseInt(fileds[2]));
            tableBean.setPname("");
            tableBean.setFlag("order");

            k.set(fileds[1]);
        }else{
            String[] fileds = line.split("\t");
            tableBean.setOrder_id("");
            tableBean.setP_id(fileds[0]);
            tableBean.setAmount(0);
            tableBean.setPname(fileds[1]);
            tableBean.setFlag("pd");

            k.set(fileds[0]);
        }

        context.write(k,tableBean);

    }
}
