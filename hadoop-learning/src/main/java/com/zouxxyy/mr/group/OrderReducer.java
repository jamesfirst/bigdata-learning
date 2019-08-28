package com.zouxxyy.mr.group;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;


public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        // 有2个空对象，开始时默认存该组第一个(k,v)的反序列值，接着每执行一次iterator.next()，会序列化下一对(k,v)写入空对象中
        // 一直只有两个对象，只是不断在反序列写入

        // 由于是该组第一个，已经序列化好，可以直接写入context中
        // context.write(key, NullWritable.get());

        // 遍历values，本质是循环调用iterator.next()，把分组里的key，value逐对反序列化，再执行context.write写入context中
        // for (NullWritable value : values) {
        //     context.write(key, NullWritable.get());
        //}

        // 测试：取出每组前两名
        Iterator<NullWritable> iterator = values.iterator();
        for (int i = 0; i < 2; i ++) {
            if(iterator.hasNext()) {
                context.write(key, iterator.next());
            }
        }
    }
}
