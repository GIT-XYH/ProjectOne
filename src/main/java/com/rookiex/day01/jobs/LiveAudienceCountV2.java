package com.rookiex.day01.jobs;

import com.rookiex.day01.deserializer.MyKafkaDeserializerSchema;
import com.rookiex.day01.pojo.DataBean;
import com.rookiex.day01.udfs.AudienceAggregateFunction;
import com.rookiex.day01.udfs.AudienceProcessWindowFunction;
import com.rookiex.day01.udfs.JsonToBeanWithIdFunction;
import com.rookiex.day01.utils.FlinkUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 实时的统计累计关众、实时在线观众
 *
 * 为了写入数据减少对外部的数据库压力，我们使用窗口，将数据进行增量聚合，这样输出的数据就变少了，对数据库的压力也变少了
 */
public class LiveAudienceCountV2 {

    public static void main(String[] args) throws Exception{

        DataStream<Tuple2<String, String>> kafkaStreamWithId = FlinkUtils.createKafkaStreamWithId(args[0], MyKafkaDeserializerSchema.class);

        SingleOutputStreamOperator<DataBean> beanStream = kafkaStreamWithId.process(new JsonToBeanWithIdFunction());

        SingleOutputStreamOperator<DataBean> filtered = beanStream.filter(bean -> "liveEnter".equals(bean.getEventId()) || "liveLeave".equals(bean.getEventId()));

        KeyedStream<DataBean, String> keyedStream = filtered.keyBy(bean -> bean.getProperties().get("anchor_id").toString());

        //5s输出一次结果
        WindowedStream<DataBean, String, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        SingleOutputStreamOperator<Tuple4<String, String, Integer, Integer>> res = windowedStream.aggregate(new AudienceAggregateFunction(), new AudienceProcessWindowFunction());

        res.print();
        //或者将结果写入到Redis、MySQL

        FlinkUtils.env.execute();
    }

}
