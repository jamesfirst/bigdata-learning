package com.zouxxyy.mr.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 实现topn方法：让所有数据分到同一组
 */

public class FlowComparator extends WritableComparator {

    protected FlowComparator() {
        super(FlowBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return 0;
    }
}
