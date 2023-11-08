package com.jd.flink.pojo;

import java.util.Objects;

public class AggKey {
/**
 *@program: flink-start
 *@description: key class
 *@author: zhaochen
 *@create: 2022-09-06 11:06
 * pojo数据类型：
 * The class must be public.
 *
 * It must have a public constructor without arguments (default constructor).
 *
 * All fields are either public or must be accessible through getter and setter functions. For a field called foo the getter and setter methods must be named getFoo() and setFoo().
 *
 * The type of a field must be supported by a registered serializer.
 */
    public long timeStamp;
    public String domain;
    public String url;

    public AggKey() {
    }

    public AggKey(long timeStamp, String domain, String url) {
        this.timeStamp = timeStamp;
        this.domain = domain;
        this.url = url;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggKey aggKey = (AggKey) o;
        return timeStamp == aggKey.timeStamp &&
                Objects.equals(domain, aggKey.domain) &&
                Objects.equals(url, aggKey.url);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeStamp, domain, url);
    }

    @Override
    public String toString() {
        return "AggKey{" +
                "timeStamp=" + timeStamp +
                ", domain='" + domain + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}
