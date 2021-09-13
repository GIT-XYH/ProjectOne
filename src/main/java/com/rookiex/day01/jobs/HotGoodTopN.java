package com.rookiex.day01.jobs;

import com.rookiex.day01.pojo.DataBean;
import com.rookiex.day01.pojo.ItemEventCount;
import com.rookiex.day01.udfs.HotGoodAggregateFunction;
import com.rookiex.day01.udfs.HotGoodTopNFunction;
import com.rookiex.day01.udfs.HotGoodWindowFunction;
import com.rookiex.day01.udfs.JsonToBeanFunction;
import com.rookiex.day01.utils.FlinkUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

public class HotGoodTopN {

    public static void main(String[] args) throws Exception {

        DataStream<String> lines = FlinkUtils.createKafkaStream(args[0], SimpleStringSchema.class);

        SingleOutputStreamOperator<DataBean> beanStream = lines.process(new JsonToBeanFunction());

        //过滤数据
        SingleOutputStreamOperator<DataBean> filtered = beanStream.filter(bean -> bean.getEventId().startsWith("product"));

        //提取evenTime生成WaterMark
        SingleOutputStreamOperator<DataBean> beanStreamWithWaterMark = filtered.assignTimestampsAndWatermarks(WatermarkStrategy.<DataBean>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<DataBean>() {
            @Override
            public long extractTimestamp(DataBean dataBean, long l) {
                return dataBean.getTimestamp();
            }
        }));

        //现将数据整理
        SingleOutputStreamOperator<Tuple3<String, String, String>> tp3Steam = beanStreamWithWaterMark.map(new MapFunction<DataBean, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(DataBean dataBean) throws Exception {
                String categoryId = dataBean.getProperties().get("category_id").toString();
                String productId = dataBean.getProperties().get("product_id").toString();
                String eventId = dataBean.getEventId();
                return Tuple3.of(categoryId, productId, eventId);
            }
        });
        //按照指定的条件进行KeyBy
        KeyedStream<Tuple3<String, String, String>, Tuple3<String, String, String>> keyedStream = tp3Steam.keyBy(new KeySelector<Tuple3<String, String, String>, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> getKey(Tuple3<String, String, String> tp) throws Exception {
                return tp;
            }
        });

        //划分窗口
        WindowedStream<Tuple3<String, String, String>, Tuple3<String, String, String>, TimeWindow> window = keyedStream.window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)));

        //划分Window后，调用window的aggregate方法，传入两个Function
        //第一个function完成增量聚合的功能
        //第二个function，当窗口触发后，将聚合的结果和Window的起始时间、结束时间，封装到ItemEventCount中输出
        SingleOutputStreamOperator<ItemEventCount> aggregateStream = window.aggregate(new HotGoodAggregateFunction(), new HotGoodWindowFunction());

        //先KeyBy，按照窗前的起始时间、结束时间、分类ID、事件ID
        KeyedStream<ItemEventCount, Tuple4<Long, Long, String, String>> keyedStream2 = aggregateStream.keyBy(new KeySelector<ItemEventCount, Tuple4<Long, Long, String, String>>() {
            @Override
            public Tuple4<Long, Long, String, String> getKey(ItemEventCount itemEventCount) throws Exception {
                return Tuple4.of(itemEventCount.windowStart, itemEventCount.windowEnd, itemEventCount.categoryId, itemEventCount.eventId);
            }
        });

        //使用ProcessFunction + Timer实现排序
        SingleOutputStreamOperator<ItemEventCount> res = keyedStream2.process(new HotGoodTopNFunction());

        res.print();

        FlinkUtils.env.execute();


    }

}
