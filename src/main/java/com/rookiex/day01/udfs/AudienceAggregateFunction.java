package com.rookiex.day01.udfs;


import com.rookiex.day01.pojo.DataBean;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class AudienceAggregateFunction implements AggregateFunction<DataBean, Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>> {

    @Override
    public Tuple3<String, Integer, Integer> createAccumulator() {
        return Tuple3.of(null, 0, 0);
    }

    /**
     * 窗口内相同组每输入一条数据调用一次add方法
     * @param value
     * @param accumulator
     * @return
     */
    @Override
    public Tuple3<String, Integer, Integer> add(DataBean value, Tuple3<String, Integer, Integer> accumulator) {
        String roomId = value.getProperties().get("room_id").toString();
        String eventId = value.getEventId();
        if("liveEnter".equals(eventId)) {
            accumulator.f1 = accumulator.f1 + 1;
            accumulator.f2 = accumulator.f2 + 1;
        } else if ("liveLeave".equals(eventId)) {
            accumulator.f2 = accumulator.f2 - 1;
        }
        accumulator.f0 = roomId;
        return accumulator;
    }

    @Override
    public Tuple3<String, Integer, Integer> getResult(Tuple3<String, Integer, Integer> accumulator) {
        return accumulator;
    }

    /**
     * merge方法只有在SessionWindow时可能会调用，其他类型的Window不会调用
     * @param a
     * @param b
     * @return
     */
    @Override
    public Tuple3<String, Integer, Integer> merge(Tuple3<String, Integer, Integer> a, Tuple3<String, Integer, Integer> b) {
        return null;
    }
}
