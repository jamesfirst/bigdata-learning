package com.zouxxyy.mr.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

// map做数据封装
public class RJMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private OrderBean orderBean = new OrderBean();
    private String filename;

    // map先执行一次setup
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        // 获取文件名
        FileSplit fs = (FileSplit) context.getInputSplit();
        filename = fs.getPath().getName();
    }

    // 每次处理k,v执行一次map
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(" ");
        if(filename.equals("order.txt")) {
            orderBean.setId(fields[0]);
            orderBean.setPid(fields[1]);
            orderBean.setAmount(Integer.parseInt(fields[2]));
            // 两个原因：序列化，且orderbean反复使用
            orderBean.setPname("");
        }
        else {
            orderBean.setId("");
            orderBean.setPid(fields[0]);
            orderBean.setAmount(0);
            orderBean.setPname(fields[1]);
        }
        context.write(orderBean, NullWritable.get());
    }
}
