package com.atguigu.realimepublisher.mapper;


import com.atguigu.realimepublisher.bean.OrderHourAmount;

import java.util.List;

public interface OrderMapper {

    //当天订单金额
    public Double getOrderAmount(String date);
    //分时统计订单金额
    public List<OrderHourAmount> getOrderHourAmount(String date);
}
