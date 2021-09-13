package com.rookiex.day01.udfs;

import com.rookiex.day01.pojo.DataBean;
import com.rookiex.day01.utils.Constants;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class AudienceProcessFunctionV3 extends KeyedProcessFunction<Tuple2<String, String>, DataBean, Tuple4<String, String, Integer, Integer>> {

    private transient MapState<String, Long> sessionEnterTimeState;
    private transient MapState<String, Integer> userCountState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Long> sessionStateDescriptor = new MapStateDescriptor<>("session-enter-time-state", String.class, Long.class);
        MapState<String, Long> mapState = getRuntimeContext().getMapState(sessionStateDescriptor);

        MapStateDescriptor<String, Integer> userCountStateDescriptor = new MapStateDescriptor<>("user-count-state", String.class, Integer.class);
        userCountState = getRuntimeContext().getMapState(userCountStateDescriptor);

    }

    @Override
    public void processElement(DataBean value, Context ctx, Collector<Tuple4<String, String, Integer, Integer>> out) throws Exception {

        String sessionId = value.getProperties().get("live_session").toString();
        String eventId = value.getEventId();
        Long timestamp = value.getTimestamp();
        //进入
        if ("liveEnter".equals(eventId)) {
            //将用户的直播间seesionid和事件加入到mapState
            sessionEnterTimeState.put(sessionId, timestamp);
            //注册一个定时器(如果统计一起产生的数据，那么就必须使用EventTime)
            ctx.timerService().registerProcessingTimeTimer(timestamp + 60000);
        } else if ("liveLeave".equals(eventId)) {
            Long enterTime = sessionEnterTimeState.get(sessionId);
            if (enterTime != null) {
                //移除进入直播间的session和对应的时间
                sessionEnterTimeState.remove(sessionId);
            }
        }

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<String, String, Integer, Integer>> out) throws Exception {

        ArrayList<String> sessionIds = new ArrayList<String>();
        //统计sessionEnterTimeState中的数据中用户停留的时间是否大于指定时间
        Iterator<Map.Entry<String, Long>> iterator = sessionEnterTimeState.iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, Long> entry = iterator.next();
            Long enterTime = entry.getValue();
            if (timestamp - enterTime >= 60000) {
                //String totalCountKey = "TOTAL_COUNT_KEY";
                Integer totalCount = userCountState.get(Constants.TOTAL_USER_COUNT);
                if (totalCount == null) {
                    totalCount = 0;
                }
                totalCount += 1;
                userCountState.put(Constants.TOTAL_USER_COUNT, totalCount);
                //从map中将对应会话的数据删除
                //sessionEnterTimeState.remove(entry.getKey());
                sessionIds.add(entry.getKey());
            }
        }
        for (String sessionId : sessionIds) {
            sessionEnterTimeState.remove(sessionId);
        }

        //输出结果
        out.collect(Tuple4.of(ctx.getCurrentKey().f0, ctx.getCurrentKey().f1, userCountState.get(Constants.TOTAL_USER_COUNT), 0));
    }
}
