package com.hnust.mr.cache;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class DistributedCacheMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    private Map<String,String> pdMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //获取缓存
        URI[] cacheFiles = context.getCacheFiles();
        //获取缓存里的第一个文件，并得到路径
        String path = cacheFiles[0].getPath().toString();

        //根据路径创建输入流
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));

        String line;
        //
        while (StringUtils.isNotEmpty(line = reader.readLine())){
            String[] fileds = line.split("\t");
            //将pid作为key，pname作为value写入pdMap
            pdMap.put(fileds[0],fileds[1]);
        }

        //关流
        IOUtils.closeStream(reader);

    }

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取一行
        String line = value.toString();

        String[] fileds = line.split("\t");
        //获取pid
        String pId = fileds[1];

        //根据pid去pdMap里查找相应的pname
        String pName = pdMap.get(pId);

        //拼接
        line = line + "\t" +pName;
        k.set(line);
        //输出
        context.write(k,NullWritable.get());
    }
}
