package com.zouxxyy.mr.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * 在map中实现join，适用与一个表小，一个大时
 */

public class MJDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[] { "data/input/reduceJoin/order.txt", "data/output" };

        // 1 获取Job对象
        Job job = Job.getInstance(new Configuration());

        // 2 设置类路径
        job.setJarByClass(MJDriver.class);

        // 3 关联Map
        job.setMapperClass(MJMapper.class);

        // 不需要reduce
        job.setNumReduceTasks(0);

        // 添加要缓存小表的路径
        job.addCacheFile(URI.create("data/input/reduceJoin/pd.txt"));


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b  ? 0 : 1);
    }
}
