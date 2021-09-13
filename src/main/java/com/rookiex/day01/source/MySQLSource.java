package com.rookiex.day01.source;

import com.rookiex.day01.pojo.GiftBean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;

public class MySQLSource extends RichSourceFunction<GiftBean> {

    private boolean flag = true;

    public Connection connection;

    public Long lastQueryTime = 0L;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/db2?characterEncoding=utf-8", "root", "xu12100108");
    }

    @Override
    public void run(SourceContext<GiftBean> ctx) throws Exception {

        PreparedStatement preparedStatement = connection.prepareStatement("SELECT id, name, points FROM tb_live_gift WHERE updateTime > ?");
        while (flag) {
            preparedStatement.setDate(1, new Date(lastQueryTime));
            ResultSet resultSet = preparedStatement.executeQuery();
            lastQueryTime = System.currentTimeMillis();
            while (resultSet.next()) {
                int id = resultSet.getInt(1);
                String name = resultSet.getString(2);
                double point = resultSet.getDouble(3);
                GiftBean giftBean = GiftBean.of(id, name, point);
                ctx.collect(giftBean);
            }
            resultSet.close();
            Thread.sleep(5000);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
