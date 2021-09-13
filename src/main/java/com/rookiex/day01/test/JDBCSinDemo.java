package com.rookiex.day01.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author RookieX
 * @Date 2021/8/30 7:35 下午
 * @Description:
 * 测试从 JDBC 往 Mysql 中写入数据
 */
public class JDBCSinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //u001, zhangsan, 18
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> tupleStream = lines.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple3.of(split[0], split[1], Integer.parseInt(split[2]));
            }
        });
        tupleStream.addSink(
                JdbcSink.sink(
                        //指定要执行的SQL
                        "insert into test_24 (id, name, age) values (?, ?, ?)",

                        new JdbcStatementBuilder<Tuple3<String, String, Integer>>() {
                            //将对应的参数放进去
                            @Override
                            public void accept(PreparedStatement preparedStatement, Tuple3<String, String, Integer> tp) throws SQLException {
                                preparedStatement.setString(1, tp.f0);
                                preparedStatement.setString(2, tp.f1);
                                preparedStatement.setInt(3, tp.f2);
                            }
                        },

                        JdbcExecutionOptions.builder()
                                .withBatchSize(1000) //批量写入数据的条数
                                .withBatchIntervalMs(200) //多长时间写一次
                                .withMaxRetries(5) //出现问题最多重试 5 次
                                .build(),

                        //指定连接参数
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://localhost:3306/db2?characterEncoding=UTF-8")
                                .withDriverName("com.mysql.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("xu12100108")
                                .build()
                )
        );

        env.execute();

    }
}
