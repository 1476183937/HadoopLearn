package com.hnust.mr.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//测试WritableComparable：自定义排序
public class FlowCountSortDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"E://input//phone_result.txt","E://input//output5.txt"};
        //1.获取Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2.设置jar类
        job.setJarByClass(FlowCountSortDriver.class);
        //3.关联map和reduce
        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReducer.class);
        //4.设置map阶段的输出的key和value的类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        //5.设置最终输出阶段key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //6.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //设置数据分区
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);

        //7.提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
