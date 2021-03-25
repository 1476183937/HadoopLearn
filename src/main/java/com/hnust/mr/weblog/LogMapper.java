package com.hnust.mr.weblog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable,Text,Text, NullWritable> {

    Text k = new Text();

    //数据清洗：只留下长度大于11的数据
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        boolean result = parseLog(line,context);
        if (!result){
             return;
        }

        k.set(line);
        context.write(k,NullWritable.get());
    }

    //
    private boolean parseLog(String line, Context context) {

        String[] fileds = line.split(" ");
        if (fileds.length > 11){
            //获取计数器
            context.getCounter("map","true").increment(1);
            return true;
        }else {
            context.getCounter("map","false").increment(1);
            return false;
        }
    }
}
