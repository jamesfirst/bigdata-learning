package com.zouxxyy.mr.group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderComparator extends WritableComparator {

    // 数据都是以序列化的形式存在，用的时候再反序列
    // 先生成2个空对象，再把OrderBean反序列化进去
    protected OrderComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        OrderBean oa = (OrderBean) a;
        OrderBean ob = (OrderBean) b;

        // 自定义的分组比较方法
        return oa.getOrderId().compareTo(ob.getOrderId());

    }
}
