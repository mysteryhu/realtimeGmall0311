package com.atguigu.gmall0311.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0311.realtime.bean.OrderInfo
import com.atguigu.gmall0311.realtime.util.MyKafkaUtil
import com.atguigu.realimeGmall0311.common.constant.GmallConstants
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderApp {


  def main(args: Array[String]): Unit = {
    //订单交易额  //保存到HBase中
    val conf = new SparkConf().setAppName("order_app").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(5))

    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER,ssc)


    //敏感字段脱敏 电话 收件人  地址 ...
    val orderInfoDStream: DStream[OrderInfo] = inputDStream.map { record =>
      val jsonStr: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonStr, classOf[OrderInfo])
      //补充时间戳
      //切分时间  2019-08-13 12:55:12
      val dateTimeArr: Array[String] = orderInfo.create_time.split(" ")
      //取到日期
      orderInfo.create_date = dateTimeArr(0)
      //取到小时
      val hourStr: String = dateTimeArr(1).split(":")(0)
      orderInfo.create_hour = hourStr
      //脱敏
      val tuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
      orderInfo.consignee_tel = tuple._1 + "*******"

      orderInfo
    }

    //保存到Hbase
    //1.先在Hbase建好表
    //2.保存到Hbase+Phoenix
    import org.apache.phoenix.spark._  //隐式转换
    orderInfoDStream.foreachRDD{rdd=>
      rdd.saveToPhoenix("GMALL0311_ORDER_INFO",Seq("ID","PROVINCE_ID",
        "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY",
        "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS",
        "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO",
        "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        new Configuration,Some("hadoop102,hadoop103,hadoop104:2181")
      )
    }

    //练习: 增加一个字段, 是否是用户首次消费 IS_FIRST_CONSUMER 是 1 不是 0  //百万级别 redis  mysql  aliyun(tablestore) 超大用户 hbase
    ssc.start()
    ssc.awaitTermination()
  }

}
