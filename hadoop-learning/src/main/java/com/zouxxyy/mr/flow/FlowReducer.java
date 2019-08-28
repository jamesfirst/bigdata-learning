package com.zouxxyy.mr.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean sumFLow = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long sumUpFlow = 0;
        long sumDownFlow = 0;

        for (FlowBean value : values) {
            sumUpFlow += value.getUpFlow();
            sumDownFlow += value.getdownFlow();
        }

        sumFLow.setUpFlow(sumUpFlow);
        sumFLow.setdownFlow(sumDownFlow);
        sumFLow.setSumFlow(sumDownFlow + sumUpFlow);

        context.write(key, sumFLow);
    }
}
