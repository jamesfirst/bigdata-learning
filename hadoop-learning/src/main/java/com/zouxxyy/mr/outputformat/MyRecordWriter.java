package com.zouxxyy.mr.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyRecordWriter extends RecordWriter<LongWritable, Text> {

    private FSDataOutputStream zxy;
    private FSDataOutputStream other;

    // 自定义的初始化方法，开流
    public void initialize(TaskAttemptContext taskAttemptContext) throws IOException {
        String outdir = taskAttemptContext.getConfiguration().get(FileOutputFormat.OUTDIR);
        FileSystem fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());

        zxy = fileSystem.create(new Path(outdir + "/zxy.log"));
        other = fileSystem.create(new Path(outdir + "/other.log"));
    }

    @Override
    public void write(LongWritable longWritable, Text text) throws IOException, InterruptedException {

        String out = text.toString() + "\n"; // 要加换行
        if(out.contains("zouxxyy")) {
            zxy.write(out.getBytes());
        }
        else {
            other.write(out.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        IOUtils.closeStream(zxy);
        IOUtils.closeStream(other);
    }
}
