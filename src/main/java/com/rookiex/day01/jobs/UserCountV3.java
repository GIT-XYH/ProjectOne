package com.rookiex.day01.jobs;

import com.rookiex.day01.deserializer.MyKafkaDeserializerSchema;
import com.rookiex.day01.pojo.DataBean;
import com.rookiex.day01.udfs.IsNewUserFunction;
import com.rookiex.day01.udfs.JsonToBeanWithIdFunction;
import com.rookiex.day01.utils.FlinkUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * 根据数据中的设备ID，实时计算当前设备是新用户、还是老用户
 *
 * 使用设备ID进行keyBy，相同设备ID的数据一定会进入到同一个分区中，当前设备ID第一次出现就保存到状态中，以后出现，根据状态进行判断
 * 1.可以使用ValueState<HashSet<String> 将用户ID保存到HashSet中，但是用户很多，HashSet就会占用大量空间，会导致checkpoint需要的时间过长，甚至内存溢出
 * 2.将用户ID保存到布隆过滤器中, 可能会有数据倾斜
 */
public class UserCountV3 {

    public static void main(String[] args) throws Exception {

        DataStream<Tuple2<String, String>> dataStreamWithId = FlinkUtils.createKafkaStreamWithId(args[0], MyKafkaDeserializerSchema.class);

        SingleOutputStreamOperator<DataBean> dataBeanStream = dataStreamWithId.process(new JsonToBeanWithIdFunction());

        //如果使用设备ID进行KeyBy，使用ValueState保存布隆过滤器，会每个用户对应一个布隆过滤器，会占用太大的内存
        //退而求其次：按照设备类型进行keyBy，相同设备ID的数据一定会进入到同一个分区（可以会有数据倾斜）
        KeyedStream<DataBean, String> keyedStream = dataBeanStream.keyBy(DataBean::getDeviceType);

        SingleOutputStreamOperator<DataBean> res = keyedStream.process(new IsNewUserFunction());

        res.print();


        FlinkUtils.env.execute();

    }
}
