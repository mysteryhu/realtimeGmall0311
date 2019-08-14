package com.atguigu.realimepublisher.service.serviceImpl;

import com.atguigu.realimepublisher.mapper.DauMapper;
import com.atguigu.realimepublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Date 2019/8/14
 * @Version JDK 1.8
 **/
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    //指定日期活跃用户数量
    @Override
    public int getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    //指定日期各时段活跃用户
    @Override
    public Map getDauHours(String date) {

        HashMap dauHourMap = new HashMap<>();
        //查询的结果 为 loghour count(*)
        List<Map> dauHourList = dauMapper.selectDauTotalHourMap(date);
        for (Map map : dauHourList) {
            dauHourMap.put(map.get("LH"),map.get("CT"));
        }
        return dauHourMap;
    }


}
