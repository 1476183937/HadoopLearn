package com.hnust.mr.outputfoemat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream atguiguOutput;
    private FSDataOutputStream ortherOutput;

    public FilterRecordWriter(TaskAttemptContext taskAttemptContext) {

        FileSystem fs;

        //创建输入输出流
        try {
            fs = FileSystem.get(taskAttemptContext.getConfiguration());
            atguiguOutput = fs.create(new Path("E://input/log/atguigu.log"));
            ortherOutput = fs.create(new Path("E://input/log/orther.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {

        if (text.toString().contains("atguigu")){
            //对于包含atguigu的输出到atguigu.log文件
            atguiguOutput.write(text.toString().getBytes());
        }else {
            //对于不包含atguigu的输出到orther.log文件
            ortherOutput.write(text.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        IOUtils.closeStream(atguiguOutput);
        IOUtils.closeStream(ortherOutput);
    }
}
