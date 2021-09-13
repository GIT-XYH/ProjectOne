package com.rookiex.day01.udfs;

import com.rookiex.day01.pojo.DataBean;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class AudienceCountFunction extends KeyedProcessFunction<String, DataBean, Tuple4<String, String, Integer, Integer>> {

    private transient MapState<String, Integer> audienceCountState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Integer> stateDescriptor = new MapStateDescriptor<>("audience-state", String.class, Integer.class);
        audienceCountState = getRuntimeContext().getMapState(stateDescriptor);
    }


    /**
     * @param value DataBean
     * @param ctx
     * @param out Tuple4<主播ID、开播的日期、累计观众数量、在线观众数量>
     * @throws Exception
     */
    @Override
    public void processElement(DataBean value, Context ctx, Collector<Tuple4<String, String, Integer, Integer>> out) throws Exception {

        String eventId = value.getEventId();
        //使用room_id即直播间ID，每一次开播，都会变化
        String roomId = value.getProperties().get("room_id").toString();
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

        //进入直播间
        if("liveEnter".equals(eventId)) {
            totalAudience += 1;
            //在线用户数量+1
            onlineAudience += 1;
        } else if("liveLeave".equals(eventId)) {
            onlineAudience -= 1;
        }
        audienceCountState.put(roomIdAndTotalKey, totalAudience);
        audienceCountState.put(roomIdAndOnlineKey, onlineAudience);

        //输出数据
        out.collect(Tuple4.of(ctx.getCurrentKey(), roomId, totalAudience, onlineAudience));
    }
}
