package com.rookiex.day01.udfs;

import com.alibaba.fastjson.JSON;
import com.rookiex.day01.pojo.DataBean;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 将从Kafka中读取的JSON字符串转成Pojo对象
 */
//没有 keyBy 的ProcessFunction只能处理数据
public class JsonToBeanWithIdFunction extends ProcessFunction<Tuple2<String, String>, DataBean> {


    @Override
    public void processElement(Tuple2<String, String> value, Context ctx, Collector<DataBean> out) throws Exception {
        try {
            String line = value.f1;
            DataBean dataBean = JSON.parseObject(line, DataBean.class);
            dataBean.setId(value.f0);
            out.collect(dataBean);
        } catch (Exception e) {

        }
    }
}
