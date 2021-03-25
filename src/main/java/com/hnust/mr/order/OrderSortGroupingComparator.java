package com.hnust.mr.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderSortGroupingComparator extends WritableComparator {

    public OrderSortGroupingComparator() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;

        //对map阶段的输出数据进行处理，id不同就按升序排序，只要id相同就输入到同一个Reduce，不管价格同不同
        int result;
        if (aBean.getOrder_id() > bBean.getOrder_id()){
            result = 1;
        }else if (aBean.getOrder_id() < bBean.getOrder_id()){
            result = -1;
        }else {
            result = 0;
        }

        return result;
    }
}
