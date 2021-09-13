package com.rookiex.day01.udfs;

import com.rookiex.day01.pojo.DataBean;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 根据设备ID判断用户是否是一个新用户
 */
public class IsNewUserFunction extends KeyedProcessFunction<String, DataBean, DataBean> {

    //状态一般都要使用 transient 修饰, 不参与序列化
    private transient ValueState<BloomFilter<String>> bloomFilterState;

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化会恢复状态
        ValueStateDescriptor<BloomFilter<String>> stateDescriptor = new ValueStateDescriptor<>("uid-state", TypeInformation.of(new TypeHint<BloomFilter<String>>() {}));
        bloomFilterState = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public void processElement(DataBean input, Context ctx, Collector<DataBean> out) throws Exception {

        String deviceId = input.getDeviceId();
        BloomFilter<String> bloomFilter = bloomFilterState.value();
        if (bloomFilter == null) {
            bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 2000000);
        }
        if(!bloomFilter.mightContain(deviceId)) {
            bloomFilter.put(deviceId);
            input.setIsN(1); //标记为新用户
        }
        out.collect(input);
    }
}
