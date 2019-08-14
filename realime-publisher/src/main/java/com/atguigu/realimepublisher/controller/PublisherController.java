package com.atguigu.realimepublisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realimepublisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Date 2019/8/14
 * @Version JDK 1.8
 **/
@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;
    //数据格式
    // [{"id":"dau","name":"新增日活","value":1200},
    // {"id":"new_mid","name":"新增设备","value":233}]
    @GetMapping("realtime-total")
    public String realtimeHourDate(@RequestParam("date") String date){
        //日活总数
        long dauTotal = publisherService.getDauTotal(date);

        List<Map> totalList = new ArrayList<>();
        Map dauMap = new HashMap<>();

        dauMap.put("id","new_mid");
        dauMap.put("name","新增设备");
        dauMap.put("value",dauTotal);
        totalList.add(dauMap);
        //新增设备数
        HashMap newMidMap = new HashMap<>();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",122);
        totalList.add(newMidMap);
        return JSON.toJSONString(totalList);
    }

    //数据格式 : 分时统计
    //{"yesterday":{"11":383,"12":123,"17":88,"19":200 },
    //    "today":{"12":38,"13":1233,"17":123,"19":688 }}
    @GetMapping("realtime-hour")
    public String realtimeHourDate(@RequestParam("id") String id,@RequestParam("date") String date){

        if("dau".equals(id)){
            //当天的分时统计
            Map<String,Long> dauHoursToday = publisherService.getDauHours(date);
            //获取前一天的日期
            String yesterdayString = getYesterdayString(date);
            //前一天的分时统计
            Map<String,Long> dauHoursYesterday = publisherService.getDauHours(yesterdayString);
            HashMap dauMap = new HashMap();
            dauMap.put("today",dauHoursToday);
            dauMap.put("yesterday",dauHoursYesterday);

            return JSON.toJSONString(dauMap);
        }else {
            //其他业务
        }
        return null;
    }


    //取得指定日期前一天的日期
    private  String getYesterdayString (String todayStr){

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date today = null;
        try {
            today = dateFormat.parse(todayStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Date yesday = DateUtils.addDays(today, -1);
            String yesdayString = dateFormat.format(yesday);
            return  yesdayString;
    }

}
