package com.rookiex.day01.udfs;

import com.rookiex.day01.pojo.DataBean;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

/**
 * 在该Function中使用OperatorState
 */
public class IsNewUserFunctionV2 implements MapFunction<DataBean, DataBean>, CheckpointedFunction {

    private transient ListState<BloomFilter<String>> listState;
    private BloomFilter<String> bloomFilter;

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //初始化或恢复状态
        ListStateDescriptor<BloomFilter<String>> stateDescriptor = new ListStateDescriptor<>("uid-state", TypeInformation.of(new TypeHint<BloomFilter<String>>() {}));
        listState = context.getOperatorStateStore().getListState(stateDescriptor);
        if (context.isRestored()) {
            for (BloomFilter<String> bloomFilter : listState.get()) {
                this.bloomFilter = bloomFilter;
            }
        }
    }

    @Override
    public DataBean map(DataBean dataBean) throws Exception {
        if (bloomFilter == null) {
            bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 500000);
        }
        String deviceId = dataBean.getDeviceId();
        if (!bloomFilter.mightContain(deviceId)) {
            bloomFilter.put(deviceId);
            dataBean.setIsN(1);
        }
        return dataBean;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        listState.clear();
        listState.add(bloomFilter);
    }

}
