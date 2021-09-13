package com.rookiex.day01.jobs;

import com.rookiex.day01.deserializer.MyKafkaDeserializerSchema;
import com.rookiex.day01.pojo.DataBean;
import com.rookiex.day01.udfs.AudienceProcessFunctionV3;
import com.rookiex.day01.udfs.JsonToBeanWithIdFunction;
import com.rookiex.day01.utils.FlinkUtils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * 实时的统计累计关众（再直播间停留60秒或以上的用户才算一个有效的观众）、实时在线观众
 *
 * 可以使用ProcessFunction + Timer实现增量聚合和停留数据的判断
 */
public class LiveAudienceCountV3 {

    public static void main(String[] args) throws Exception{

        DataStream<Tuple2<String, String>> kafkaStreamWithId = FlinkUtils.createKafkaStreamWithId(args[0], MyKafkaDeserializerSchema.class);

        SingleOutputStreamOperator<DataBean> beanStream = kafkaStreamWithId.process(new JsonToBeanWithIdFunction());

        SingleOutputStreamOperator<DataBean> filtered = beanStream.filter(bean -> "liveEnter".equals(bean.getEventId()) || "liveLeave".equals(bean.getEventId()));

        KeyedStream<DataBean, Tuple2<String, String>> keyedStream = filtered.keyBy(new KeySelector<DataBean, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(DataBean bean) throws Exception {
                return Tuple2.of(bean.getProperties().get("anchor_id").toString(),  bean.getProperties().get("room_id").toString());
            }
        });

        //使用ProcessFunction + Timmer
        SingleOutputStreamOperator<Tuple4<String, String, Integer, Integer>> res = keyedStream.process(new AudienceProcessFunctionV3());

        res.print();

        FlinkUtils.env.execute();
    }

}
