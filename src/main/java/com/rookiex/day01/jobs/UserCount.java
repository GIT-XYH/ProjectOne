package com.rookiex.day01.jobs;
/**
 * @Author RookieX
 * @Date 2021/8/29 7:41 下午
 * @Description:
 * 统计新用户的数量
 */

import com.rookiex.day01.pojo.DataBean;
import com.rookiex.day01.udfs.JsonToBeanFunction;
import com.rookiex.day01.utils.Constants;
import com.rookiex.day01.utils.FlinkUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;


public class UserCount {

    public static void main(String[] args) throws Exception{

        DataStream<String> lines = FlinkUtils.createKafkaStream(args[0], SimpleStringSchema.class);
        //尽量不要写内部类, 写入 udf 中, 看起来清晰易懂
        SingleOutputStreamOperator<DataBean> beanStream = lines.process(new JsonToBeanFunction());
        //所以我们统计的是 appLaunch 这种事件类型的新用户, 第一次打开 app 就会判断
        SingleOutputStreamOperator<DataBean> filtered = beanStream.filter(bean -> Constants.AppLaunch.equals(bean.getEventId()));

        //z 总共有多少新用户
        filtered.map(new MapFunction<DataBean, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(DataBean value) throws Exception {
                return Tuple2.of(value.getIsNew(), 1);

            }
        }).keyBy(t -> t.f0).sum(1).filter(t -> t.f0 == 1).print();

        //按照下载渠道统计新用户
        filtered.flatMap(
                new FlatMapFunction<DataBean, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(DataBean value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        if (value.getIsNew() == 1) {
                            out.collect(Tuple2.of(value.getReleaseChannel(), 1));
                        }
                    }
                }
        ).keyBy(t -> t.f0).sum(1).print();


        //按照下载渠道统计新用户
//        filtered.filter(new FilterFunction<DataBean>() {
//            @Override
//            public boolean filter(DataBean value) throws Exception {
//                if (value.getIsNew() == 1) {
//                    return true;
//                }
//                return false;
//            }
//        }).map(new MapFunction<DataBean, Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> map(DataBean value) throws Exception {
//                String channel = value.getReleaseChannel();
//                if (value.getIsNew() == 1) {
//
//                }
//                return Tuple2.of(channel, 1);
//            }
//
//        }).keyBy(t -> t.f0).sum(1).print();

        //getReleaseChannel和getDeviceType两个维度
        filtered.flatMap(new FlatMapFunction<DataBean, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(DataBean value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String channel = value.getReleaseChannel();
                String deviceType = value.getDeviceType();
                out.collect(Tuple2.of(channel + "-" + deviceType, 1));
            }
        }).keyBy(t -> t.f0).sum(1).print();


        FlinkUtils.env.execute();

    }
}
