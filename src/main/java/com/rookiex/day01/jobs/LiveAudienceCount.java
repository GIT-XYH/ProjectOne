package com.rookiex.day01.jobs;

import com.rookiex.day01.deserializer.MyKafkaDeserializerSchema;
import com.rookiex.day01.pojo.DataBean;
import com.rookiex.day01.udfs.AudienceCountFunction;
import com.rookiex.day01.udfs.JsonToBeanWithIdFunction;
import com.rookiex.day01.utils.FlinkUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * 实时的统计累计关众、实时在线观众
 */
public class LiveAudienceCount {

    public static void main(String[] args) throws Exception{

        DataStream<Tuple2<String, String>> kafkaStreamWithId = FlinkUtils.createKafkaStreamWithId(args[0], MyKafkaDeserializerSchema.class);

        SingleOutputStreamOperator<DataBean> beanStream = kafkaStreamWithId.process(new JsonToBeanWithIdFunction());

        SingleOutputStreamOperator<DataBean> filtered = beanStream.filter(bean -> "liveEnter".equals(bean.getEventId()) || "liveLeave".equals(bean.getEventId()));

        KeyedStream<DataBean, String> keyedStream = filtered.keyBy(bean -> bean.getProperties().get("anchor_id").toString());

        SingleOutputStreamOperator<Tuple4<String, String, Integer, Integer>> res = keyedStream.process(new AudienceCountFunction());



        res.print();
        //将计算好的结果写入到Redis后输出到控制台
        //res.writeToSocket()

        //存在一个问题，如果来一条计算一次，并且还要输出一次，这样对外部的数据库压力比较大
        //怎样改进
        //划分滚动窗口，滚动窗口进行聚合，仅会累加当前窗口中的数据，必须要累加历史数据
        //keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(1))).red


        FlinkUtils.env.execute();
    }

}
