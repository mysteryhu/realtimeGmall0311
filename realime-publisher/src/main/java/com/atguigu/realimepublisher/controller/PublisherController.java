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

        return JSON.toJSONString(totalList);
    }

    @GetMapping("realtime-hours")
    public String realtimeHourDate(@RequestParam("id") String id,@RequestParam("date") String date){
        JSONObject jsonObject = null;
        if("dau".equals(id)){
            Map dauHours = publisherService.getDauHours(date);
            jsonObject = new JSONObject();
            jsonObject.put("today",dauHours);
            String yesterdayString = getYesterdayString(date);
            Map dauHoursYesterday = publisherService.getDauHours(yesterdayString);
            jsonObject.put("yesterday",dauHoursYesterday);
        }
        return jsonObject.toJSONString();
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
