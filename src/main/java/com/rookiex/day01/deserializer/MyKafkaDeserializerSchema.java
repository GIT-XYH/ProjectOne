package com.rookiex.day01.deserializer;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.curator4.com.google.common.base.Charsets;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @Author RookieX
 * @Date 2021/8/30 6:38 下午
 * @Description:
 * 自己定义的一个 Kafka 反序列化器, 从 Kafka 中去读取数据时, 可以获取相应的 topic, partition, offset
 * 使用 topic-partition-offset 作为数据的唯一 id
 */

//反序列化后的数据类型 Tuple2, 唯一 id + 内容JSON序列化之后的二进制的字符串 String
public class MyKafkaDeserializerSchema implements KafkaDeserializationSchema<Tuple2<String, String>> {

    //是否读完了
    @Override
    public boolean isEndOfStream(Tuple2<String, String> stringStringTuple2) {
        return false;
    }


    @Override
    public Tuple2<String, String> deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        String topic = consumerRecord.topic();
        int partition = consumerRecord.partition();
        long offset = consumerRecord.offset();
        //从 kafka 中读取的数据
//        byte[] value = consumerRecord.value();
        String line = new String(consumerRecord.value(), Charsets.UTF_8);
        return Tuple2.of(topic + "-" + partition + "-" + offset, line);
    }

    //读出来的数据是什么类型的
    @Override
    public TypeInformation<Tuple2<String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        });
    }
}
