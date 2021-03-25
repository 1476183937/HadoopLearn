package com.hnust.mr.outputfoemat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FilterReducer extends Reducer<Text, NullWritable, Text,NullWritable> {

    Text k = new Text();

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        //读取一行
        String line = key.toString();
        //拼接换行
        line = line + "\r\n";
        k.set(line);

        context.write(k,NullWritable.get());
    }
}
