package com.rookiex.day01.udfs;

import com.rookiex.day01.pojo.DataBean;
import com.rookiex.day01.pojo.GiftBean;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class GiftBroadcastProcessFunction extends BroadcastProcessFunction<DataBean, GiftBean, Tuple5<String, String, String, Integer, Double>> {

    private MapStateDescriptor<Integer, Tuple2<String, Double>> broadcastStateDescriptor;

    public GiftBroadcastProcessFunction(){}

    public GiftBroadcastProcessFunction(MapStateDescriptor<Integer, Tuple2<String, Double>> broadcastStateDescriptor) {
        this.broadcastStateDescriptor = broadcastStateDescriptor;
    }

    @Override
    public void processElement(DataBean value, ReadOnlyContext ctx, Collector<Tuple5<String, String, String, Integer, Double>> out) throws Exception {

        String anchorId = value.getProperties().get("anchor_id").toString();
        String roomId = value.getProperties().get("room_id").toString();
        Integer giftId = Integer.parseInt(value.getProperties().get("gift_id").toString());
        ReadOnlyBroadcastState<Integer, Tuple2<String, Double>> broadcastState = ctx.getBroadcastState(broadcastStateDescriptor);
        Tuple2<String, Double> tp = broadcastState.get(giftId);
        String name = null;
        Double point = null;
        if(tp != null) {
            name = tp.f0;
            point = tp.f1;
        }
        out.collect(Tuple5.of(anchorId, roomId, name, 1, point));
    }

    @Override
    public void processBroadcastElement(GiftBean value, Context ctx, Collector<Tuple5<String, String, String, Integer, Double>> out) throws Exception {

        BroadcastState<Integer, Tuple2<String, Double>> broadcastState = ctx.getBroadcastState(broadcastStateDescriptor);
        if (value.getDeleted() == 1) {
            broadcastState.remove(value.id);
        } else {
            broadcastState.put(value.id, Tuple2.of(value.name, value.point));
        }
    }
}
