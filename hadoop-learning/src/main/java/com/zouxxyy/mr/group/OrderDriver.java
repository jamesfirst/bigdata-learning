package com.zouxxyy.mr.group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 排序 -> 分组 -> reduce
 * 可以用自定义的Comparator，定义分组规则，默认是对key相同的分组
 * 注意：WritableComparable<OrderBean>是用于key排序的，Comparator是用于key排序后key分组的
 */

public class OrderDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[] { "data/input/group", "data/output" };

        // 1 获取Job对象
        Job job = Job.getInstance(new Configuration());

        // 2 设置类路径
        job.setJarByClass(OrderDriver.class);

        // 3 关联Map和Reduce类
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        // 添加Comparator
        job.setGroupingComparatorClass(OrderComparator.class);

        // 4 设置Mapper输出的key和value类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5 设置数据输出的key和value类型
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 6. 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b  ? 0 : 1);

    }
}
