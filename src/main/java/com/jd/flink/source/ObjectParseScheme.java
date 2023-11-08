package com.jd.flink.source;

import com.alibaba.fastjson.JSON;
import com.jd.flink.pojo.AggKey;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
/*把输入的json直接解析 也可以拿到bytes数组后进行别的处理*/
public class ObjectParseScheme implements DeserializationSchema<AggKey> {
    @Override
    public AggKey deserialize(byte[] bytes) throws IOException {
        return JSON.parseObject(bytes, AggKey.class);
    }

    @Override
    public boolean isEndOfStream(AggKey aggKey) {
        return false;
    }

    @Override
    public TypeInformation<AggKey> getProducedType() {
        return TypeInformation.of(AggKey.class);
    }

}
