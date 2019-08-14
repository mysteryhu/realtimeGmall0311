package com.atguigu.gmall0311.realtime.bean

case class Startuplog(
                        mid:String,     //唯一设备号
                        uid:String,     //用户id
                        appid:String,   //应用id
                        area:String,   //地区
                        os:String,     //系统
                        ch:String,      //下载渠道
                        logType:String,  //日志类型
                        vs:String,      //版本号
                        var logDate:String,//当日日期(天)
                        var logHour:String,//当日日期(时)
                        var tm:Long

                      ){

}


