package com.rookiex.day01.udfs;

import com.alibaba.fastjson.JSON;
import com.rookiex.day01.pojo.DataBean;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 将从Kafka中读取的JSON字符串转成Pojo对象
 */
//没有 keyBy 的ProcessFunction只能处理数据
public class JsonToBeanFunction extends ProcessFunction<String, DataBean> {

    @Override
    public void processElement(String value, Context ctx, Collector<DataBean> out) throws Exception {

        try {
            //将 json 解析成对象
            DataBean dataBean = JSON.parseObject(value, DataBean.class);
            out.collect(dataBean);
        } catch (Exception e) {
            //e.printStackTrace();
            //将错误数据单独的记录
        }
    }
}
