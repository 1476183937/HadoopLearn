package com.hnust.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text,FlowBean,Text,FlowBean> {

    private Text k = new Text();
    private FlowBean v = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
//        13568436656	2481	24681
//        13568436656	1116	954
        long upFlow = 0;
        long downFlow = 0;
        k.set(key);
        //遍历所有bean，统计上行、下行流量
        for (FlowBean flowBean : values) {
            upFlow += flowBean.getUpFlow();
            downFlow += flowBean.getDownFlow();
        }
        //封装对象，对上行、下行流量求和
        v.set(upFlow,downFlow);
        context.write(k,v);


    }
}
