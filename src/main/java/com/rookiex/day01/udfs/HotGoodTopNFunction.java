package com.rookiex.day01.udfs;

import com.rookiex.day01.pojo.ItemEventCount;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class HotGoodTopNFunction extends KeyedProcessFunction<Tuple4<Long, Long, String, String>, ItemEventCount, ItemEventCount> {

    private transient ValueState<List<ItemEventCount>> valueState;

    //数据来了, 不直接输出, 先攒起来, 等定时器时间到了再触发
    //value 中装的是 list
    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<List<ItemEventCount>> stateDescriptor = new ValueStateDescriptor<>("lst-state", TypeInformation.of(new TypeHint<List<ItemEventCount>>() {}));
        valueState = getRuntimeContext().getState(stateDescriptor);
    }

    //key 相同的进入到 processElement 中
    @Override
    public void processElement(ItemEventCount value, Context ctx, Collector<ItemEventCount> out) throws Exception {
        List<ItemEventCount> lst = valueState.value();
        if (lst == null) {
            lst = new ArrayList<>();
        }
        lst.add(value);
        valueState.update(lst);
        //注册定时器
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);


    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<ItemEventCount> out) throws Exception {

        List<ItemEventCount> lst = valueState.value();
        lst.sort(new Comparator<ItemEventCount>() {
            @Override
            public int compare(ItemEventCount o1, ItemEventCount o2) {
                return Long.compare(o2.count, o1.count);
            }
        });
        for (int i = 0; i < Math.min(lst.size(), 3); i++) {
            out.collect(lst.get(i));
        }
        valueState.clear();


    }
}
