package com.hnust.mr.cache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class DistributedCacheDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        args = new String[]{"E://input/order","E://input/output12"};
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(DistributedCacheDriver.class);

        //因为没有reduce阶段，只关联map即可，同时也不用设置map阶段的输出的key和value的类型
        job.setMapperClass(DistributedCacheMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //加载缓存数据，将pd.txt文件存到缓存中
        job.addCacheFile(new URI("file:///E://input/order/pd.txt"));
        //因为不需要reduce，所以设置reduceTask数量为0
        job.setNumReduceTasks(0);

        job.waitForCompletion(true);

    }
}
