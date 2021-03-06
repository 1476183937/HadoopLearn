package com.hnust.mr.nline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NlineDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"E://input/e.txt","E://input/output3.txt"};
        //1.获取job对象
        Configuration conf = new Configuration();
        //设置以空格切割，默认是以"\t"切割，
        Job job = Job.getInstance(conf);
        //设置按3行切成一片
        NLineInputFormat.setNumLinesPerSplit(job,3);

        job.setInputFormatClass(NLineInputFormat.class);
        //2.设置jar类
        job.setJarByClass(NlineDriver.class);

        //3.关联map和Reducer
        job.setMapperClass(NlineMapper.class);
        job.setReducerClass(NlineReducer.class);
        //4.设置map阶段的输出数据的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5.设置最终输出数据的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //6.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //7。提交
        job.waitForCompletion(true);
    }
}
