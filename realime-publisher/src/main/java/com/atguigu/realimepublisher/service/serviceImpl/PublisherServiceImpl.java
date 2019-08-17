package com.atguigu.realimepublisher.service.serviceImpl;

import com.atguigu.realimepublisher.bean.OrderHourAmount;
import com.atguigu.realimepublisher.mapper.DauMapper;
import com.atguigu.realimepublisher.mapper.OrderMapper;
import com.atguigu.realimepublisher.service.PublisherService;
import org.jcodings.util.Hash;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Date 2019/8/14
 * @Version JDK 1.8
 **/
@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;
    //指定日期活跃用户数量


    @Override
    public Long getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    //指定日期各时段活跃用户
    @Override
    public Map<String,Long> getDauHours(String date) {

        HashMap dauHourMap = new HashMap<>();
        //查询的结果 为 loghour count(*)
        List<Map> dauHourList = dauMapper.selectDauTotalHourMap(date);
        for (Map map : dauHourList) {
            dauHourMap.put(map.get("LOGHOUR"),map.get("CT"));
        }
        return dauHourMap;
    }

    //查询当天订单金额
    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.getOrderAmount(date);
    }

    //查询分时订单金额
    @Override
    public Map<String, Double> getOrderHourAmount(String date) {
        //把List集合转成Map
        HashMap<String,Double> orderHourMap = new HashMap<>();
        List<OrderHourAmount> orderHourAmountList = orderMapper.getOrderHourAmount(date);
        for (OrderHourAmount orderHourAmount : orderHourAmountList) {
            orderHourMap.put(orderHourAmount.getCreateHour(),orderHourAmount.getSumOrderAmount());
        }
        return orderHourMap;
    }


}
