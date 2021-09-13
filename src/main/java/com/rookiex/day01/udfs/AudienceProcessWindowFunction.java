package com.rookiex.day01.udfs;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AudienceProcessWindowFunction extends ProcessWindowFunction<Tuple3<String, Integer, Integer>, Tuple4<String, String, Integer, Integer>, String, TimeWindow> {

    private transient MapState<String, Integer> audienceCountState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Integer> stateDescriptor = new MapStateDescriptor<>("audience-state", String.class, Integer.class);
        audienceCountState = getRuntimeContext().getMapState(stateDescriptor);
    }

    /**
     * process窗口触发后，每一个组会调用一次process
     * @param key key的条件，主播ID
     * @param context
     * @param elements Widow触发时，一个组数据的数据，可以是一条，也可以是多条
     * @param out 输出的数据
     * @throws Exception
     */
    @Override
    public void process(String key, Context context, Iterable<Tuple3<String, Integer, Integer>> elements, Collector<Tuple4<String, String, Integer, Integer>> out) throws Exception {
        //窗口增量聚合，当窗口触发后输出的结果，一个组会输出一条
        Tuple3<String, Integer, Integer> tp = elements.iterator().next();
        String roomId = tp.f0;
        String roomIdAndOnlineKey = roomId + "_online";
        //在线用户
        Integer onlineAudience = audienceCountState.get(roomIdAndOnlineKey);
        if (onlineAudience == null) {
            onlineAudience = 0;
        }

        //累计观众数量
        String roomIdAndTotalKey = roomId + "_total";
        Integer totalAudience = audienceCountState.get(roomIdAndTotalKey);
        if(totalAudience == null) {
            totalAudience = 0;
        }

        totalAudience += tp.f1;
        onlineAudience += tp.f2;

        audienceCountState.put(roomIdAndOnlineKey, onlineAudience);
        audienceCountState.put(roomIdAndTotalKey, totalAudience);

        //输出数据
        out.collect(Tuple4.of(key, roomId, totalAudience, onlineAudience));

    }


}
