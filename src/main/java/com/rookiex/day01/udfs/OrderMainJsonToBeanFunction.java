package com.rookiex.day01.udfs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.rookiex.day01.pojo.OrderMain;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class OrderMainJsonToBeanFunction extends ProcessFunction<String, OrderMain> {


    @Override
    public void processElement(String line, Context ctx, Collector<OrderMain> out) throws Exception {

        //json字符串转对象 + flatMap + filter
        try {
            //json字符串转对象
            JSONObject jsonObject = JSON.parseObject(line);
            String type = jsonObject.getString("type");
            if (type.equals("INSERT") || type.equals("UPDATE")) {
                JSONArray jsonArray = jsonObject.getJSONArray("data");
                for (int i = 0; i < jsonArray.size(); i++) {
                    OrderMain orderMain = jsonArray.getObject(i, OrderMain.class);
                    orderMain.setType(type); //设置操作类型
                    out.collect(orderMain);
                }
            }
        } catch (Exception e) {
            //e.printStackTrace();
            //记录错误的数据
        }


    }
}
