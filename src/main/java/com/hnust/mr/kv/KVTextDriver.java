package com.hnust.mr.kv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 统计每行开头单词相同的行数
 */
public class KVTextDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //输入例子：zhangsan 1
        //         zhangsan 1
        //输出：zhangsan 2
        args = new String[]{"E://input/a.txt","E://input/output1.txt"};
        //1.获取job对象
        Configuration conf = new Configuration();
        //设置以空格切割，默认是以"\t"切割，
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");
        Job job = Job.getInstance(conf);
        //2.设置jar类
        job.setJarByClass(KVTextDriver.class);

        //3.关联map和Reducer
        job.setMapperClass(KVTextMapper.class);
        job.setReducerClass(KVTextReducer.class);
        //4.设置map阶段的输出数据的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5.设置最终输出数据的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        //6.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //7。提交
        job.waitForCompletion(true);

    }
}
