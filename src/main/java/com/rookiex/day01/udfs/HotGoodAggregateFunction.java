package com.rookiex.day01.udfs;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;


/**
 * 这个方法的作用就是累加次数, 完成增量聚合的功能
 */
public class HotGoodAggregateFunction implements AggregateFunction<Tuple3<String, String, String>, Integer, Integer> {

    /**
     * 初始值
     * @return
     */
    @Override
    public Integer createAccumulator() {
        return 0;
    }

    /**
     *
     * @param tp
     * @param acc
     * @return 中间累加的结果
     */
    @Override
    public Integer add(Tuple3<String, String, String> tp, Integer acc) {
        return acc += 1;
    }

    @Override
    public Integer getResult(Integer acc) {
        return acc;
    }


    //只有会话窗口才有 merge 方法
    @Override
    public Integer merge(Integer integer, Integer acc1) {
        return null;
    }
}
