package com.atguigu.realimelogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realimeGmall0311.common.constant.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

/**
 * @Date 2019/8/12
 * @Version JDK 1.8
 **/
@Slf4j
@RestController          //@Controller + @ResponseBody
public class LoggerController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @PostMapping("log")
    //@ResponseBody  作用:把return 返回的内容视为字符串而不是页面
    // = @RequestMapping(name = "/log",method = RequestMethod.POST)
    public  String doLog(@RequestParam("logString") String logString){
        //TODO 1.给生成的日志补充时间戳
        //把字符串json化
        JSONObject jsonObject = JSON.parseObject(logString);
        //加上时间
        jsonObject.put("tm",System.currentTimeMillis());
        //TODO 2.写日志(用于离线采集)
        //json转字符串
        String logJson = jsonObject.toJSONString();
        //控制台打印日志
        log.info(logJson);

        //TODO 3.发送给kafka
        //用的是spring-kafka
        if( "startup".equals(jsonObject.getString("type")) ){
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP,logJson);
        }else {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT,logJson);
        }

        return "success";
    }
}
