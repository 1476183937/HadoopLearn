package com.hnust.mr.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderSortReducer extends Reducer<OrderBean, NullWritable,OrderBean,NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        /*
        如输入如下，已按价格排好序，输出第一个即为几个最大者
        * 0000001	222.8
        * 0000001	33.8
        * */
        //对于已经处理好的数据，只输出第一个，就是id相同的输入中的价格最大者
        context.write(key,NullWritable.get());
    }
}
