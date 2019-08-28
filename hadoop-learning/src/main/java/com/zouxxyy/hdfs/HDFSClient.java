package com.zouxxyy.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class HDFSClient {

    private FileSystem fs;

    @Before
    public void before() throws IOException, InterruptedException {
        // 获取HDFS的抽象对象
        fs = FileSystem.get(URI.create("hdfs://localhost:9000"), new Configuration(), "zxy");
    }

    @Test
    public void put() throws IOException, InterruptedException {

        Configuration configuration = new Configuration();

        configuration.setInt("dfs.replication", 1);

        fs = FileSystem.get(URI.create("hdfs://localhost:9000"), configuration, "zxy");

        // 本地文件上传到HDFS
        fs.copyFromLocalFile(new Path("data/inputWordCount/1.txt"), new Path("/"));
    }

    @Test
    public void get() throws IOException{

         // HDFS文件下载到本地
         fs.copyToLocalFile(new Path("/1.txt"), new Path("./"));
    }

    @Test
    public void rename() throws IOException{

        // HDFS重命名
        fs.rename(new Path("/1.txt"), new Path("/2.txt"));
    }

    @Test
    public void delete() throws IOException{

        // HDFS删除
        boolean delete = fs.delete(new Path("/1.txt"), true);
        if (delete) {
            System.out.println("删除成功");
        }
        else{
            System.out.println("删除失败");
        }
    }

    @Test
    public void append() throws IOException{

        // HDFS 文件追加测试
        FSDataOutputStream append = fs.append(new Path("/1.txt"), 1024);
        FileInputStream open = new FileInputStream("data/inputWordCount/1.txt");
        IOUtils.copyBytes(open, append, 1024, true);
    }

    @Test
    public void ls() throws IOException{

        // fileStatuses包含文件和文件夹
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : fileStatuses) {
            if(fileStatus.isFile()) {
                System.out.println("文件:");
                System.out.println(fileStatus.getPath());
                System.out.println(fileStatus.getOwner());
            }
            else {
                System.out.println("文件夹:");
                System.out.println(fileStatus.getModificationTime());
                System.out.println(fileStatus.getPermission());
            }
        }
    }

    @Test
    public void listFiles() throws IOException {

        // 注意listFiles方法只能得到文件
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);

        while (files.hasNext()) {
            LocatedFileStatus file = files.next();

            System.out.println("===========================");
            System.out.println(file.getPath());
            System.out.println("块信息：");
            BlockLocation[] blockLocations = file.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                System.out.print("块在: ");
                for(String host : hosts) {
                    System.out.println(host + " ");
                }
            }
        }
    }

    @After
    public void after() throws IOException {
        fs.close();
    }

}
