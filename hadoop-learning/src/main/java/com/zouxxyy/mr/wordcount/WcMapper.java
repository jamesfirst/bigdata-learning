package com.zouxxyy.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // 防止大量产生对象，使用成员变量
    private Text word = new Text();
    private IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] words = line.split(" ");

        for (String word : words) {

            this.word.set(word);

            context.write(this.word, this.one); // 这里我觉得很奇怪，为什么已填的成员变量属性不会改变，我觉得应该是这个write会把数据写进某个东西里面
        }
    }
}
