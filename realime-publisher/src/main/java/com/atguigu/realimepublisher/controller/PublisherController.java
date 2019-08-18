package com.atguigu.realimepublisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.realimepublisher.bean.Option;
import com.atguigu.realimepublisher.bean.Stat;
import com.atguigu.realimepublisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
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
        //日交易额
        Double orderAmount = publisherService.getOrderAmount(date);

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
        //订单金额
        HashMap orderAmountMap = new HashMap<>();
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        orderAmountMap.put("value",orderAmount);
        totalList.add(orderAmountMap);
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
        }else if("order_amount".equals(id)) {
            //订单金额的分时统计
            Map<String, Double> orderHourAmountMap = publisherService.getOrderHourAmount(date);
            //获取前一天的日期
            String yesterdayString = getYesterdayString(date);
            //System.out.println(yesterdayString);
            //前一天的分时统计
            Map<String, Double> yesorderHourAmountMap = publisherService.getOrderHourAmount(yesterdayString);
            System.out.println(yesorderHourAmountMap);
            System.out.println(orderHourAmountMap);
            Map orderMap = new HashMap();
            orderMap.put("today",orderHourAmountMap);
            orderMap.put("yesterday",yesorderHourAmountMap);

            return JSON.toJSONString(orderMap);
        }else{ //其他业务

        }
        return null;
    }


    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,@RequestParam("keyword") String keyword,@RequestParam("startpage") int startPage,@RequestParam("size") int size){
        //根据参数查询es
        Map<String, Object> saleDetailMap = publisherService.getSaleDetailFromES(date, keyword, startPage, size);
        long total = (long) saleDetailMap.get("total");
        List saleList = (List) saleDetailMap.get("saleList");
        Map genderMap = (Map) saleDetailMap.get("genderMap");
        Map ageMap = (Map) saleDetailMap.get("ageMap");


        long maleCount = (long) genderMap.get("M");
        long femaleCount = (long) genderMap.get("F");
        //男女占比
        double maleRatio = Math.round(maleCount * 1000D / total) / 10D;   //67
        double femaleRatio = Math.round(femaleCount * 1000D / total) / 10D;

        ArrayList genderOptionList = new ArrayList();
        genderOptionList.add(new Option("男",maleRatio));
        genderOptionList.add(new Option("女",femaleRatio));
        //饼图的选项1:性别占比
        Stat genderStat = new Stat("性别占比", genderOptionList);

        //饼图的选项2:年龄占比
        Long age_20count = 0L;
        Long age20_30count = 0L;
        Long age30_count = 0L;
        for (Object o : ageMap.entrySet()) {
            Map.Entry entry=(Map.Entry) o;
            String ageString = (String) entry.getKey();
            Long ageCount =(Long)entry.getValue();
            if(Integer.parseInt(ageString)<20 ){
                age_20count+=ageCount;
            }else if(Integer.parseInt(ageString)>=20 && Integer.parseInt(ageString)<30){
                age20_30count+=ageCount;
            }else {
                age30_count+=ageCount;
            }
        }

        //转换成百分比
        double age_20Ratio = Math.round(age_20count * 1000D / total) / 10D;
        double age20_30Ratio = Math.round(age20_30count * 1000D / total) / 10D;
        double age30_Ratio = Math.round(age30_count * 1000D / total) / 10D;

        //年龄选项
        List ageOptionList = new ArrayList();
        ageOptionList.add(new Option("20",age_20Ratio));
        ageOptionList.add(new Option("20_30",age20_30Ratio));
        ageOptionList.add(new Option("30_",age30_Ratio));

        Stat ageStat = new Stat("年龄占比", ageOptionList);
        //stat列表
        List statList = new ArrayList();
        statList.add(genderOptionList);
        statList.add(ageOptionList);

        Map finalResultMap  = new HashMap();
        finalResultMap.put("total",total);
        finalResultMap.put("stat",statList);
        finalResultMap.put("detail",saleList);

        return JSON.toJSONString(finalResultMap);
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
