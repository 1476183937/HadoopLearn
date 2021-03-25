package com.hnust.mr.compress;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;
import java.util.Arrays;
import java.util.Optional;

//解压缩文件
public class TestCompress {
    public static void main(String[] args) throws ClassNotFoundException, IOException {
//        compress("E://input/a.txt","org.apache.hadoop.io.compress.BZip2Codec");
//        compress("E://input/a.txt","org.apache.hadoop.io.compress.GzipCodec");

        decompress("E://input/a.zip");
    }

    //解压缩
    private static void decompress(String filename) throws IOException {

        //检验是否可解压缩
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(filename));
        if (codec == null){
            System.out.println("不能解压缩。。。");
            return;
        }


        //获取输入流
        CompressionInputStream cis = codec.createInputStream(new FileInputStream(new File(filename)));
        //获取输出流
        FileOutputStream fos = new FileOutputStream(new File("E://input/decompress"));

        //流的对拷
        IOUtils.copyBytes(cis,fos,1024*1024,false);
        
        //关闭资源
        IOUtils.closeStream(cis);
        IOUtils.closeStream(fos);


    }

    private static void compress(String filename, String method) throws ClassNotFoundException, IOException {

        //获取输入流
        FileInputStream fis = new FileInputStream(new File(filename));

        //使用反射
        Class<?> codecClass = Class.forName(method);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, new Configuration());
        //根据文件名和后缀名创建普通输出流
        FileOutputStream fos = new FileOutputStream(new File(filename + codec.getDefaultExtension()));
        //利用codec创建压缩的输出流
        CompressionOutputStream cos = codec.createOutputStream(fos);

        //流的对拷
        IOUtils.copyBytes(fis,cos,1024*1024*5,false);

        //关闭流
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(cos);

    }
}
