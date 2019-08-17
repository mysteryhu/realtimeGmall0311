package com.atguigu.realimepublisher.bean;

import lombok.Data;

/**
 * @Date 2019/8/16
 * @Version JDK 1.8
 **/
@Data //自动生成get 和 set 方法
public class OrderHourAmount {

    private String createHour;

    private Double sumOrderAmount;
}
