package com.rookiex.day01.test;

import com.rookiex.day01.serializer.MyKafkaSerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * Flink读取Kafka中的数据，然后将处理后的数据再写回到Kafka
 * 并且保证ExactlyOnce
 *
 */
public class KafkaToKafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //EXACTLY_ONCE，对Source和Sink是有要求
        //Source可以记录偏移量
        //Sink支持事务
        env.enableCheckpointing(30000,  CheckpointingMode.EXACTLY_ONCE);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("transaction.timeout.ms", "600000");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("tp-in", new SimpleStringSchema(), properties);
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);
        DataStreamSource<String> lines = env.addSource(kafkaConsumer);

        //将数据进行过滤（ETL）
        SingleOutputStreamOperator<String> filtered = lines.filter(line -> !line.contains("error"));
        //再将数据写入到Kafka中

        String topic = "tp-out";

        FlinkKafkaProducer<String> flinkKafkaProducer = new FlinkKafkaProducer<>(
                topic,
                new MyKafkaSerializationSchema(topic),
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );

        filtered.addSink(flinkKafkaProducer);

        env.execute();

    }

}
