package com.hnust.mr.test;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @Title:
 * @Author: ggh
 * @Date: 2020/11/26 22:52
 */
public class WordCountRecordWriter extends RecordWriter<Text, IntWritable> {

    FSDataOutputStream fis;

    public WordCountRecordWriter(TaskAttemptContext context) throws IOException {

        //创建输出流
        FileSystem fs = FileSystem.get(context.getConfiguration());
        fis = fs.create(new Path("C:\\Users\\ggh\\Desktop\\count.txt"));

    }

    @Override
    public void write(Text key, IntWritable value) throws IOException, InterruptedException {

        fis.write((key.toString() + ":" +value.toString()+"\t").getBytes());

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(fis);
    }
}
