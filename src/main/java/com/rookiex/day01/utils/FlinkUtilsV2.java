package com.rookiex.day01.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FlinkUtilsV2 {

    public static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static <T> DataStream<T> createKafkaStream(String path, Class<? extends DeserializationSchema<T>> clazz) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(path);
        String topics = parameterTool.get("input.topics");
        String groupId = parameterTool.get("group.id", "g001");
        return createKafkaStream(parameterTool, topics, groupId, clazz);
    }


    public static <T> DataStream<T> createKafkaStream(ParameterTool parameterTool, String topicNames, String groupId, Class<? extends DeserializationSchema<T>> clazz) throws Exception {



        String bootstrapServers = parameterTool.getRequired("bootstrap.servers");

        //为了容错，要开启checkpoint
        env.enableCheckpointing(parameterTool.getLong("checkpoint.interval", 60000));
        //在checkpoint时，将状态保存到HDFS存储后端
        //env.setStateBackend(new EmbeddedRocksDBStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage("file:///checkpoint-dir");
        //将job cancel后，保留外部存储的checkpoint数据（为了以后重新提交job恢复数据）
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", parameterTool.getRequired("bootstrap.servers"));
        properties.setProperty("auto.offset.reset", parameterTool.get("auto.offset.reset", "earliest"));
        properties.setProperty("group.id", groupId);

        //String topics = parameterTool.getRequired("input.topics");
        List<String> topicList = Arrays.asList(topicNames.split(","));

        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<>(
                topicList,
                clazz.newInstance(),
                properties
        );

        //在checkpoint成功后，不将偏移量写入到Kafka特殊的topic中
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        DataStreamSource<T> lines = env.addSource(kafkaConsumer);

        return lines;
    }


    public static <T> DataStream<T> createKafkaStreamWithId(String path, Class<? extends KafkaDeserializationSchema<T>> clazz) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(path);

        String bootstrapServers = parameterTool.getRequired("bootstrap.servers");

        //为了容错，要开启checkpoint
        env.enableCheckpointing(parameterTool.getLong("checkpoint.interval", 60000));
        //在checkpoint时，将状态保存到HDFS存储后端
        env.setStateBackend(new FsStateBackend(parameterTool.getRequired("checkpoint.path")));
        //将job cancel后，保留外部存储的checkpoint数据（为了以后重新提交job恢复数据）
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", parameterTool.getRequired("bootstrap.servers"));
        properties.setProperty("auto.offset.reset", parameterTool.get("auto.offset.reset", "earliest"));
        properties.setProperty("group.id", parameterTool.get("group.id", "g001"));

        String topics = parameterTool.getRequired("input.topics");
        List<String> topicList = Arrays.asList(topics.split(","));

        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<>(
                topicList,
                clazz.newInstance(),
                properties
        );

        //在checkpoint成功后，不将偏移量写入到Kafka特殊的topic中
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        DataStreamSource<T> lines = env.addSource(kafkaConsumer);

        return lines;
    }
}
