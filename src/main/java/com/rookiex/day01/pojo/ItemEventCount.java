package com.rookiex.day01.pojo;


public class ItemEventCount {

    public String categoryId;  //商品分类ID
    public String productId;     // 商品ID
    public String eventId;     // 事件类型

    public long count;  // 对应事件的次数量
    public long windowStart;  // 窗口开始时间戳
    public long windowEnd;  // 窗口结束时间戳

    public ItemEventCount(String categoryId, String productId, String eventId, long count, long windowStart, long windowEnd) {
        this.productId = productId;
        this.eventId = eventId;
        this.categoryId = categoryId;
        this.count = count;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "ItemEventCount{" +
                "categoryId='" + categoryId + '\'' +
                ", productId='" + productId + '\'' +
                ", eventId='" + eventId + '\'' +
                ", count=" + count +
                ", windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                '}';
    }
}