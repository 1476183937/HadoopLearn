package com.hnust.mr.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TwoIndexReducer extends Reducer<Text, Text,Text,Text> {

    Text v = new Text();

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //输入
        // atguigu a.txt 3
        // atguigu b.txt 2
        // atguigu c.txt 2

        //输出
        // atguigu c.txt-->2 b.txt-->2 a.txt-->3
        StringBuilder builder = new StringBuilder();

        //拼接
        for (Text value : values) {
            builder.append(value.toString().replace("\t","-->")+"\t");
        }

        v.set(builder.toString());
        context.write(key,v);

    }
}
