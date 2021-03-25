package com.hnust.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeRecordReader extends RecordReader<Text, BytesWritable> {

    private FileSplit split;
    private Configuration configuration;
    private BytesWritable value = new BytesWritable();
    private Text k = new Text();
    private boolean isProgress = true;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.split = (FileSplit) inputSplit;
        configuration = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (isProgress) {
            //设置缓冲流
            byte[] buff = new byte[(int) split.getLength()];
            FSDataInputStream fis = null;
            try {
                //通过切片来获取路径，再根据路径获取文件系统
                Path path = split.getPath();
                FileSystem fs = path.getFileSystem(configuration);
                //根据路径获取输入流来读取文件
                fis = fs.open(path);
                //利用输入流读取内容存到buff缓冲区里面,即一个文件里面的内容
                IOUtils.readFully(fis, buff, 0, buff.length);
                //将缓冲区里的内容作为value输出
                value.set(buff, 0, buff.length);
                //将文件路径名作为key输出
                String name = split.getPath().toString();
                k.set(name);
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                //关流
                IOUtils.closeStream(fis);
            }
            isProgress = false;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
