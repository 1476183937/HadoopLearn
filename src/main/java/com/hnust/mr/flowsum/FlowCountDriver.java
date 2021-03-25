package com.hnust.mr.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"E://input/phone_data.txt","E://input/output1.txt"};
        Configuration conf = new Configuration();


        //1.获取Job对象
        Job job = Job.getInstance(conf);

        //2.设置jar位置
        job.setJarByClass(FlowCountDriver.class);

        //3.关联Map和Reducer
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //4.设置Map阶段输出数据的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5.设置最终输出数据的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //---------------------------------------------------
        //设置自定义数据分区

        job.setPartitionerClass(ProvincePartitioner.class);
        //设置5个分区
        job.setNumReduceTasks(5);
        //---------------------------------------------------

        //6.设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //7.提交
        job.waitForCompletion(true);
    }
}
