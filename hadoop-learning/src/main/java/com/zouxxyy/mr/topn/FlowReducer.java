package com.zouxxyy.mr.topn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //由于所有数据在同一组，取完前10，reduce就结束了
        Iterator<Text> iterator = values.iterator();
        for (int i = 0; i < 10; i++) {
            if(iterator.hasNext()) {
                context.write(iterator.next(), key);
            }
        }
    }
}
