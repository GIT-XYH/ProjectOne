package com.rookiex.day01.serializer;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * 将写入到Kafka中的数据要进行序列化，将字符串变成二进制
 */
public class MyKafkaSerializationSchema implements KafkaSerializationSchema<String> {

    private String topic;

    public MyKafkaSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
        return new ProducerRecord<>(topic, element.getBytes(StandardCharsets.UTF_8));
    }


}
