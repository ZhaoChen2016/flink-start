package com.jd.flink.pojo;

public class AggResult {
/**
 *@program: flink-start
 *@description:
 *@author: zhao chen
 *@create: 2022-09-06 19:46
 */
    public AggValue tuple2;
    public long windowEnd;
    public AggKey aggKey;

    public AggResult() {
    }

    public AggResult(AggValue tuple2, long windowEnd, AggKey aggKey) {
        this.tuple2 = tuple2;
        this.windowEnd = windowEnd;
        this.aggKey = aggKey;
    }

    @Override
    public String toString() {
        return "AggResult{" +
                "tuple2=" + tuple2 +
                ", windowEnd=" + windowEnd +
                ", aggKey=" + aggKey +
                '}';
    }
}
