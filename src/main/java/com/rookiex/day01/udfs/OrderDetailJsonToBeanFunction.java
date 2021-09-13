package com.rookiex.day01.udfs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.rookiex.day01.pojo.OrderDetail;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class OrderDetailJsonToBeanFunction extends ProcessFunction<String, OrderDetail> {

    @Override
    public void processElement(String line, Context ctx, Collector<OrderDetail> out) throws Exception {


        try {
            JSONObject jsonObject = JSON.parseObject(line);
            String type = jsonObject.getString("type");
            if (type.equals("INSERT") || type.equals("UPDATE")) {
                JSONArray jsonArray = jsonObject.getJSONArray("data");
                for (int i = 0; i < jsonArray.size(); i++) {
                    OrderDetail orderDetail = jsonArray.getObject(i, OrderDetail.class);
                    orderDetail.setType(type); //设置操作类型
                    out.collect(orderDetail);
                }
            }
        } catch (Exception e) {
            //e.printStackTrace();
            //记录错误的数据
        }

    }
}
