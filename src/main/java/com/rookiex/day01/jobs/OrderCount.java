package com.rookiex.day01.jobs;

import com.rookiex.day01.pojo.OrderDetail;
import com.rookiex.day01.pojo.OrderMain;
import com.rookiex.day01.udfs.OrderDetailJsonToBeanFunction;
import com.rookiex.day01.udfs.OrderMainJsonToBeanFunction;
import com.rookiex.day01.utils.FlinkUtilsV2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 读取kafka中两个流的数据：tp-main、tp-detail
 *
 * 然后将两个流进行join
 */
public class OrderCount {

    public static void main(String[] args) throws Exception {

        FlinkUtilsV2.env.setParallelism(1);

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);
        DataStream<String> orderMainStream = FlinkUtilsV2.createKafkaStream(parameterTool, "tp-main", "g001", SimpleStringSchema.class);

        SingleOutputStreamOperator<OrderMain> orderMainBeanStream = orderMainStream.process(new OrderMainJsonToBeanFunction());

        DataStream<String> orderDetailStream = FlinkUtilsV2.createKafkaStream(parameterTool, "tp-detail", "g001", SimpleStringSchema.class);

        SingleOutputStreamOperator<OrderDetail> orderDetailBeanStream = orderDetailStream.process(new OrderDetailJsonToBeanFunction());

        SingleOutputStreamOperator<OrderDetail> orderDetailWithWaterMark = orderDetailBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
            @Override
            public long extractTimestamp(OrderDetail orderDetail, long l) {
                return orderDetail.getUpdate_time().getTime();
            }
        }));

        //在join之前，是对orderDetailWithWaterMark这个Stream划分窗
        //窗口的类型，长度与以后join时使用的窗口一样
        WindowedStream<OrderDetail, Long, TimeWindow> orderDetailWindow = orderDetailWithWaterMark.keyBy(d -> d.getOrder_id()).window(TumblingEventTimeWindows.of(Time.seconds(10)));
        OutputTag<OrderDetail> orderDetailOutputTag = new OutputTag<>("order-detail-late-data");
        //将迟到的数据打上标签
        orderDetailWindow.sideOutputLateData(orderDetailOutputTag);
        SingleOutputStreamOperator<OrderDetail> orderDetailStreamWithLateDate = orderDetailWindow.apply(new WindowFunction<OrderDetail, OrderDetail, Long, TimeWindow>() {
            @Override
            public void apply(Long aLong, TimeWindow window, Iterable<OrderDetail> input, Collector<OrderDetail> out) throws Exception {
            }
        });
        //orderDetailStreamWithLateDate.print();
        //根据事先定义的迟到标签的tag，获取迟到的数据
        DataStream<OrderDetail> lateOrderDetailStream = orderDetailStreamWithLateDate.getSideOutput(orderDetailOutputTag);
        SingleOutputStreamOperator<Tuple2<OrderDetail, OrderMain>> lateTupleStream = lateOrderDetailStream.map(new MapFunction<OrderDetail, Tuple2<OrderDetail, OrderMain>>() {
            @Override
            public Tuple2<OrderDetail, OrderMain> map(OrderDetail orderDetail) throws Exception {
                return Tuple2.of(orderDetail, null);
            }
        });


        SingleOutputStreamOperator<OrderMain> orderMainWithWaterMark = orderMainBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderMain>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<OrderMain>() {
            @Override
            public long extractTimestamp(OrderMain orderMain, long l) {
                return orderMain.getUpdate_time().getTime();
            }
        }));

        //将orderDetail表对应的数据流作为左表，然后使用左外连接，左表的流如果没join上，也输出
        DataStream<Tuple2<OrderDetail, OrderMain>> joinedStream = orderDetailWithWaterMark.coGroup(orderMainWithWaterMark)
                .where(d -> d.getOrder_id())
                .equalTo(m -> m.getOid())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new CoGroupFunction<OrderDetail, OrderMain, Tuple2<OrderDetail, OrderMain>>() {
                    @Override
                    public void coGroup(Iterable<OrderDetail> left, Iterable<OrderMain> right, Collector<Tuple2<OrderDetail, OrderMain>> collector) throws Exception {

                        for (OrderDetail orderDetail : left) {
                            boolean isJoined = false;
                            for (OrderMain orderMain : right) {
                                isJoined = true;
                                collector.collect(Tuple2.of(orderDetail, orderMain));
                            }
                            if (!isJoined) {
                                collector.collect(Tuple2.of(orderDetail, null));
                            }
                        }
                    }
                });

        //将两个流进行统一的处理
        joinedStream.union(lateTupleStream).map(new RichMapFunction<Tuple2<OrderDetail, OrderMain>, Tuple2<OrderDetail, OrderMain>>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public Tuple2<OrderDetail, OrderMain> map(Tuple2<OrderDetail, OrderMain> tp) throws Exception {
                if (tp.f1 == null) {
                    //查询数据库，或查询REST API
                    tp.f1 = new OrderMain(); //模拟已经查出来的数据
                }
                return tp;
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        });

        //将数据写入到ClickHouse


        joinedStream.print();


        FlinkUtilsV2.env.execute();

    }

}
