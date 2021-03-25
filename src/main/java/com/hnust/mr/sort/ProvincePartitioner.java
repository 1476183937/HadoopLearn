package com.hnust.mr.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<FlowBean,Text>  {


    @Override
    public int getPartition(FlowBean bean, Text text, int i) {
        int partition = 4;
        String prePhone = text.toString().substring(0, 3);
        if ("136".equals(prePhone)){
            partition = 0;
        }else if ("137".equals(prePhone)){
            partition = 1;
        }else if ("138".equals(prePhone)){
            partition = 2;
        }else if ("139".equals(prePhone)){
            partition = 3;
        }
        return partition;
    }
}
