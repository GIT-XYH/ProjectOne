package com.rookiex.day01.jobs;

import com.rookiex.day01.deserializer.MyKafkaDeserializerSchema;
import com.rookiex.day01.pojo.DataBean;
import com.rookiex.day01.pojo.GiftBean;
import com.rookiex.day01.source.MySQLSource;
import com.rookiex.day01.udfs.GiftBroadcastProcessFunction;
import com.rookiex.day01.udfs.JsonToBeanWithIdFunction;
import com.rookiex.day01.utils.FlinkUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.*;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class GiftCount {

    public static void main(String[] args) throws Exception {

        DataStreamSource<GiftBean> giftBeanDataStream = FlinkUtils.env.addSource(new MySQLSource());

        MapStateDescriptor<Integer, Tuple2<String, Double>> broadcastStateDescriptor = new MapStateDescriptor<>("gift-state", TypeInformation.of(Integer.class), TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {}));

        BroadcastStream<GiftBean> broadcastStream = giftBeanDataStream.broadcast(broadcastStateDescriptor);

        DataStream<Tuple2<String, String>> kafkaStreamWithId = FlinkUtils.createKafkaStreamWithId(args[0], MyKafkaDeserializerSchema.class);
        SingleOutputStreamOperator<DataBean> beanStream = kafkaStreamWithId.process(new JsonToBeanWithIdFunction());
        SingleOutputStreamOperator<DataBean> filtered = beanStream.filter(bean -> "liveReward".equals(bean.getEventId()));

        //Tuple5<主播ID，直播ID，礼物名称，数量，points>
        SingleOutputStreamOperator<Tuple5<String, String, String, Integer, Double>> tp5Stream = filtered.connect(broadcastStream).process(new GiftBroadcastProcessFunction(broadcastStateDescriptor));

        KeyedStream<Tuple5<String, String, String, Integer, Double>, Tuple2<String, String>> keyedStream = tp5Stream.keyBy(new KeySelector<Tuple5<String, String, String, Integer, Double>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple5<String, String, String, Integer, Double> value) throws Exception {
                return Tuple2.of(value.f0, value.f1);
            }
        });

        SingleOutputStreamOperator<Tuple> res = keyedStream.reduce(new ReduceFunction<Tuple5<String, String, String, Integer, Double>>() {
            @Override
            public Tuple5<String, String, String, Integer, Double> reduce(Tuple5<String, String, String, Integer, Double> value1, Tuple5<String, String, String, Integer, Double> value2) throws Exception {

                value1.f2 = null;
                value1.f3 += value2.f3;
                value1.f4 += value2.f4;

                return value1;
            }
        }).project(0, 1, 3, 4);


        res.addSink(JdbcSink.sink(
                "insert into tb_anchor_gift_point values (?, ?, ?, ?) on duplicate key update amount = ? and point = ?",
                new JdbcStatementBuilder<Tuple>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Tuple tp) throws SQLException {
                        preparedStatement.setString(1, tp.getField(0).toString());
                        preparedStatement.setString(2, tp.getField(1).toString());
                        preparedStatement.setInt(3, Integer.parseInt(tp.getField(2).toString()));
                        preparedStatement.setDouble(4, Double.parseDouble(tp.getField(2).toString()));
                        preparedStatement.setInt(5, Integer.parseInt(tp.getField(2).toString()));
                        preparedStatement.setDouble(6, Double.parseDouble(tp.getField(2).toString()));
                    }
                },
                JdbcExecutionOptions.builder().withBatchSize(5).withBatchIntervalMs(2000).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl("").build()
        ));


        tp5Stream.keyBy(t -> t.f2).sum(3).print(); //.addSink() //或调用reduce将赠送礼物的数量


        // tp5Stream.addSink()  将数据在没有聚合之前（带上各种维度），写入到ClickHouse，可以实现多维度的聚合统计。


        FlinkUtils.env.execute();

    }
}
