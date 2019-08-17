package com.atguigu.gmall0311.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0311.realtime.bean.{AlertInfo, EventInfo}
import com.atguigu.gmall0311.realtime.util.{MyESUtil, MyKafkaUtil}
import com.atguigu.realimeGmall0311.common.constant.GmallConstants
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._
object AlertApp {
  //预警
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkStreaming上下文环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")

    val ssc = new StreamingContext(conf,Seconds(5))

    val inputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT,ssc)

    val eventInfoDstream: DStream[EventInfo] = inputStream.map {
      record =>
        //封装成EventInfo
        val eventInfo: EventInfo = JSON.parseObject(record.value(), classOf[EventInfo])
        eventInfo
    }
    //缓存一下,防止mutil-thread
    eventInfoDstream.cache()
    //TODO 1 5分钟  窗口(窗口大小5min,滑动步长) window   窗口大小,数据的统计范围  滑动步长: 统计频率
    val eventWindowDstream: DStream[EventInfo] = eventInfoDstream.window(Seconds(300),Seconds(5))
    //TODO 2 同一设备 groupby mid
    val groupByMidDstream: DStream[(String, Iterable[EventInfo])] = eventWindowDstream.map(eventInfo=>(eventInfo.mid,eventInfo)).groupByKey()
    //TODO 3 用不同账号登录并领取优惠券   没有 浏览商品
    //TODO  map 变化结构(预警结构)  经过判断 把是否满足预警条件的mid 打上标签
    val checkDStream: DStream[(Boolean, AlertInfo)] = groupByMidDstream.map { case (mid, eventInfoItr) =>
      //账号清单
      val couponUidSet = new java.util.HashSet[String]()
      //商品购物券领取清单
      val itemSet = new java.util.HashSet[String]()
      //一个设备号所有操作的集合
      val eventList: util.ArrayList[String] = new java.util.ArrayList[String]()

      var flagClickItem = false
      breakable(
        for (eventInfo: EventInfo <- eventInfoItr) {
          //记录设备上所有操作的类型
          eventList.add(eventInfo.evid)
          //领购物券行为
          if (eventInfo.evid == "coupon") {
            //把账号放入set清单
            couponUidSet.add(eventInfo.uid)
            //领取的购物券记录下来
            itemSet.add(eventInfo.itemid)
          }
          //有浏览商品行为
          if (eventInfo.evid == "clickItem") {
            //添加标志
            flagClickItem = true
            //一旦有浏览商品行为,则退出本次循环
            break()
          }
        }
      )

      //判断符合预警的条件:
      // 1.点击购物券 三次及以上不同账号登录   点击购物券时涉及的登录账号>=3
      // 2. 期间没有浏览商品  events not contains clickItem
      (couponUidSet.size() >= 3 && !flagClickItem, AlertInfo(mid, couponUidSet, itemSet, eventList, System.currentTimeMillis()))
    }

    checkDStream.foreachRDD{rdd=>
      rdd.collect().foreach(println)
    }
    //TODO filter 不满足条件的过滤掉(没有标签的过滤掉)
    val filterDStream: DStream[(Boolean, AlertInfo)] = checkDStream.filter(rdd=>rdd._1)
    //去掉标签
    val alterDStream: DStream[AlertInfo] = filterDStream.map(rdd=>rdd._2)
    //保存到ES中

    alterDStream.foreachRDD{rdd=>
      rdd.foreachPartition{alterItr=>
        val list: List[AlertInfo] = alterItr.toList
        //提取主键 mid+ts   同时也利用主键去重
        val alterListWithId: List[(String, AlertInfo)] = list.map(alertInfo=>(alertInfo.mid+"_"+alertInfo.ts/1000/60,alertInfo))
        //批量保存
        MyESUtil.indexBulk(GmallConstants.ES_INDEX_ALERT,alterListWithId)
      }
    }




    ssc.start()
    ssc.awaitTermination()
  }

}
