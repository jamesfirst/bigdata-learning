package com.zouxxyy.mr.compress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 测试压缩和解压缩
 */

public class TestCompress {

    public static void main(String[] args) throws Exception {

        // 压缩测试
//		compress("data/input/wordCount/1.txt","org.apache.hadoop.io.compress.BZip2Codec");
//		compress("data/input/wordCount/1.txt","org.apache.hadoop.io.compress.GzipCodec");
//		compress("data/input/wordCount/1.txt","org.apache.hadoop.io.compress.DefaultCodec");

        // 解压测试
        decompress("data/input/wordCount/1.txt.deflate");

    }


    @SuppressWarnings({ "resource", "unchecked" })
    private static void compress(String fileName, String method) throws ClassNotFoundException, IOException {

        // 1 获取输入流，普通输入流
        FileInputStream fis = new FileInputStream(new File(fileName));

        // 通过反射获取CompressionCodec
        Class classCodec = Class.forName(method);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(classCodec, new Configuration());

        // 2 获取输出流，普通输出流->压缩的输出流
        FileOutputStream fos = new FileOutputStream(new File(fileName+codec.getDefaultExtension())); // 文件名+扩张后缀
        CompressionOutputStream cos = codec.createOutputStream(fos);

        // 3 流的对拷
        IOUtils.copyBytes(fis, cos, 1024*1024, false);

        // 4 关闭资源
        IOUtils.closeStream(cos);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }


    @SuppressWarnings("resource")
    private static void decompress(String fileName) throws IOException {

        // 1 校验是否能解压缩
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(fileName));

        if (codec == null) {
            System.out.println("can not process");
            return;
        }

        // 2 获取输入流
        FileInputStream fis = new FileInputStream(new File(fileName));
        CompressionInputStream cis = codec.createInputStream(fis);

        // 3 获取输出流
        FileOutputStream fos = new FileOutputStream(new File(fileName+".decode"));

        // 4 流的对拷
        IOUtils.copyBytes(cis, fos, 1024*1024, false);

        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(cis);
        IOUtils.closeStream(fis);
    }
}
