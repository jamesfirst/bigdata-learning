package com.zouxxyy.mr.invertindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *  倒排索引（多个job串联）
 *  计算多个文件倒单词个数，value是单词在各个文件中的个数
 */

public class IIDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[] { "data/input/invertIndex", "data/output", "data/output2" };

        // 1 获取Job对象
        Job job1 = Job.getInstance(new Configuration());

        // 2 设置类路径
        job1.setJarByClass(IIDriver.class);

        job1.setMapperClass(IIMapper1.class);
        job1.setReducerClass(IIReducer1.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        boolean b = job1.waitForCompletion(true);

        if(b) {
            // 1 获取Job对象
            Job job2 = Job.getInstance(new Configuration());

            // 2 设置类路径
            job2.setJarByClass(IIDriver.class);

            job2.setMapperClass(IIMapper2.class);
            job2.setReducerClass(IIReducer2.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));

            boolean b2 = job2.waitForCompletion(true);

            System.exit(b2  ? 0 : 1);
        }
    }
}
