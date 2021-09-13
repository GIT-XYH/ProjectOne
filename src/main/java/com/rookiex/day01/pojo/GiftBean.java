package com.rookiex.day01.pojo;

public class GiftBean {

    public Integer id;

    public String name;

    public Double point;

//    public Date createTime;
//
//    public Date updateTime;
//
    public int deleted;

    public GiftBean() {}

    public GiftBean(Integer id, String name, Double point) {
        this.id = id;
        this.name = name;
        this.point = point;
    }

    public static GiftBean of(Integer id, String name, Double point) {
        return new GiftBean(id, name, point);
    }

    public int getDeleted() {
        return deleted;
    }

    public void setDeleted(int deleted) {
        this.deleted = deleted;
    }

    @Override
    public String toString() {
        return "GiftBean{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", point=" + point +
                '}';
    }
}
