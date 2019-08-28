package com.zouxxyy.mr.sort2;

import com.zouxxyy.mr.sort.FlowBean;
import com.zouxxyy.mr.sort.SortMapper;
import com.zouxxyy.mr.sort.SortReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 按手机号码前3为进行分区，然后排序，结果为5个文件
 */

public class SortDriver2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[] { "data/inputSort", "data/output" };

        // 1 获取Job对象
        Job job = Job.getInstance(new Configuration());

        // 2 设置类路径
        job.setJarByClass(SortDriver2.class);

        // 3 关联Map和Reduce类
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        // 4 设置Mapper输出的key和value类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        // 设置分区
        job.setPartitionerClass(MyPatitioner2.class);
        job.setNumReduceTasks(5);

        // 5 设置数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 6. 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b  ? 0 : 1);

    }
}
