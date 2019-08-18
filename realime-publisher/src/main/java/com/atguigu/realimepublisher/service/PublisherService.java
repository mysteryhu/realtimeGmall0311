package com.atguigu.realimepublisher.service;


import java.util.Map;

public interface PublisherService {

         Long getDauTotal(String date );

         Map<String,Long> getDauHours(String date );

         Double getOrderAmount(String date);

         Map<String,Double> getOrderHourAmount(String date);

         Map<String, Object> getSaleDetailFromES(String date, String keyword, int pageNo, int pageSize);

}
