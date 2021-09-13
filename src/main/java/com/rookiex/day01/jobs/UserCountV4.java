package com.rookiex.day01.jobs;

import com.rookiex.day01.deserializer.MyKafkaDeserializerSchema;
import com.rookiex.day01.pojo.DataBean;
import com.rookiex.day01.udfs.IsNewUserFunctionV2;
import com.rookiex.day01.udfs.JsonToBeanWithIdFunction;
import com.rookiex.day01.utils.FlinkUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * 根据数据中的设备ID，实时计算当前设备是新用户、还是老用户
 * 按照设备ID进行keyBy，让相同设备ID的数据一定进入到同一个分区中
 * 然后在使用OperatorState，一个分区中有一个布隆过滤器（不会发生数据倾斜、更加节省资源）
 */
public class UserCountV4 {

    public static void main(String[] args) throws Exception {

        DataStream<Tuple2<String, String>> dataStreamWithId = FlinkUtils.createKafkaStreamWithId(args[0], MyKafkaDeserializerSchema.class);

        SingleOutputStreamOperator<DataBean> dataBeanStream = dataStreamWithId.process(new JsonToBeanWithIdFunction());

        //如果使用设备ID进行KeyBy
        KeyedStream<DataBean, String> keyedStream = dataBeanStream.keyBy(DataBean::getDeviceId);

        SingleOutputStreamOperator<DataBean> res = keyedStream.map(new IsNewUserFunctionV2());

        res.print();


        FlinkUtils.env.execute();

    }
}
