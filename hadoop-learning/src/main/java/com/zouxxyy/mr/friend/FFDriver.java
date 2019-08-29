package com.zouxxyy.mr.friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 计算两个人的共同关注对象
 */
public class FFDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[] { "data/input/friend", "data/output", "data/output2" };

        // 1 获取Job对象
        Job job1 = Job.getInstance(new Configuration());

        // 2 设置类路径
        job1.setJarByClass(FFDriver.class);

        job1.setMapperClass(FFMapper1.class);
        job1.setReducerClass(FFReducer1.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        boolean b = job1.waitForCompletion(true);

        if(b) {
            // 1 获取Job对象
            Job job2 = Job.getInstance(new Configuration());

            // 2 设置类路径
            job2.setJarByClass(FFDriver.class);

            job2.setMapperClass(FFMapper2.class);
            job2.setReducerClass(FFReducer2.class);

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
