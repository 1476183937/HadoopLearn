package com.hnust.mr.table;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class TableReducer extends Reducer<Text,TableBean,TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {

        List<TableBean> tableBeans = new ArrayList<>();
        TableBean pdBean = new TableBean();

        for (TableBean bean : values) {

            if ("order".equals(bean.getFlag())){//来自订单表
                TableBean tableBean = new TableBean();

                try {
                    BeanUtils.copyProperties(tableBean,bean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                tableBeans.add(tableBean);
            }else{//来自产品表

                try {
                    BeanUtils.copyProperties(pdBean,bean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }

        }

        //表的拼接
        for (TableBean tableBean : tableBeans) {
            tableBean.setPname(pdBean.getPname());
            //将数据写出去
            context.write(tableBean,NullWritable.get());
        }


    }
}
