package com.rookiex.day01.udfs;

import com.rookiex.day01.pojo.ItemEventCount;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

//<in: 增量聚合结果, out, key, window>
public class HotGoodWindowFunction implements WindowFunction<Integer, ItemEventCount, Tuple3<String, String, String>, TimeWindow> {

    @Override
    public void apply(Tuple3<String, String, String> key, TimeWindow window, Iterable<Integer> input, Collector<ItemEventCount> out) throws Exception {
        Integer count = input.iterator().next();
        long windowStart = window.getStart();
        long windowEnd = window.getEnd();
        out.collect(new ItemEventCount(key.f0, key.f1, key.f2, count, windowStart, windowEnd));
    }
}
