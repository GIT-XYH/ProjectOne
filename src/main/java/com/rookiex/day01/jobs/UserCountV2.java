package com.rookiex.day01.jobs;

import com.rookiex.day01.deserializer.MyKafkaDeserializerSchema;
import com.rookiex.day01.pojo.DataBean;
import com.rookiex.day01.udfs.JsonToBeanWithIdFunction;
import com.rookiex.day01.utils.Constants;
import com.rookiex.day01.utils.FlinkUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.text.SimpleDateFormat;
import java.util.Date;


public class UserCountV2 {

    public static void main(String[] args) throws Exception {

        DataStream<Tuple2<String, String>> dataStreamWithId = FlinkUtils.createKafkaStreamWithId(args[0], MyKafkaDeserializerSchema.class);

        SingleOutputStreamOperator<DataBean> dataBeanStream = dataStreamWithId.process(new JsonToBeanWithIdFunction());

        SingleOutputStreamOperator<DataBean> filtered = dataBeanStream.filter(bean -> Constants.AppLaunch.equals(bean.getEventId()));

        //将数据中的时间戳转成yyyyMMdd-HH格式
        //将这两个字段作为分区条件写入到ClickHouse中
        filtered.map(new MapFunction<DataBean, DataBean>() {
            private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HH");
            @Override
            public DataBean map(DataBean bean) throws Exception {
                Long timestamp = bean.getTimestamp();
                String format = dateFormat.format(new Date(timestamp));
                String[] fields = format.split("-");
                bean.setDate(fields[0]);
                bean.setHour(fields[1]);
                return bean;
            }
        }).addSink(JdbcSink.sink(
                "insert into tb_user_event(id, deviceId, eventId, isNew, os, province, channel, deviceType, eventTime, date, hour) values (?,?,?,?,?,?,?,?,?,?,?)",
                (ps, bean) -> {
                    ps.setString(1, bean.getId());
                    ps.setString(2, bean.getDeviceId());
                    ps.setString(3, bean.getEventId());
                    ps.setInt(4, bean.getIsN());
                    ps.setString(5, bean.getOsName());
                    ps.setString(6, bean.getProvince());
                    ps.setString(7, bean.getReleaseChannel());
                    ps.setString(8, bean.getDeviceType());
                    ps.setLong(9, bean.getTimestamp());
                    ps.setString(10, bean.getDate());
                    ps.setString(11, bean.getHour());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)

                        .withBatchIntervalMs(2000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://rookiex01:8123/ck?characterEncoding=utf-8")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .build()));

        FlinkUtils.env.execute();

    }
}
