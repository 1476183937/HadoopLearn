package com.hnust.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text,FlowBean> {

    @Override
    public int getPartition(Text key, FlowBean flowBean, int i) {
//        手机号136、137、138、139开头都分别放到一个独立的4个文件中，其他开头的放到一个文件中。

        int partition = 4;
        //获取手机号前三位
        String prePhoneNum = key.toString().substring(0,3);

        if ("136".equals(prePhoneNum)) {
            partition = 0;
        }else if ("137".equals(prePhoneNum)){
            partition = 1;
        }else if ("138".equals(prePhoneNum)){
            partition = 2;
        }
        else if ("139".equals(prePhoneNum)){
            partition = 3;
        }

        return partition;
    }
}
