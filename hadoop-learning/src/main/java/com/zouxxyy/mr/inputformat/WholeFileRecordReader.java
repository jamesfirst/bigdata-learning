package com.zouxxyy.mr.inputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

// 处理一个文件，把文件读成一个key-value

public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {

    private boolean notRead = true;

    private Text key = new Text();

    private BytesWritable value = new BytesWritable();

    private FSDataInputStream inputStream;

    private FileSplit fs;


    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        // 转换到文件切片
        fs = (FileSplit) inputSplit;
        Path path = fs.getPath();
        FileSystem fileSystem = path.getFileSystem(taskAttemptContext.getConfiguration());
        inputStream = fileSystem.open(path);

    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (notRead) {

            // 读文件过程
            key.set(fs.getPath().toString());

            byte[] buffer = new byte[(int)fs.getLength()];
            inputStream.read(buffer);
            value.set(buffer, 0, buffer.length);

            notRead = false;
            return true;
        } else {
            return false;
        }
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    // 获取当前进度
    public float getProgress() throws IOException, InterruptedException {
        return notRead ? 0 : 1;
    }

    public void close() throws IOException {

        IOUtils.closeStream(inputStream);
    }
}
