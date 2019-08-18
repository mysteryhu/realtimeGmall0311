package com.atguigu.gmall0311.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0311.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.gmall0311.realtime.util.{MyESUtil, MyKafkaUtil}
import com.atguigu.realimeGmall0311.common.constant.GmallConstants
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer


//import org.json4s.native.Serialization
object SaleApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("sale_app")
    val ssc = new StreamingContext(conf, Seconds(5))
    //获取kafka-Topic数据流
    val orderRecordDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)
    val orderDetailDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)
    val userRecordDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER_INFO, ssc)


    //TODO 转换样例类OrderInfo
    val orderInfoDStream: DStream[OrderInfo] = orderRecordDStream.map { record =>
      //json-> OrderInfo
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      //补充时间字段
      //补充时间戳
      //切分时间  2019-08-13 12:55:12
      val dateTimeArr: Array[String] = orderInfo.create_time.split(" ")
      //取到日期
      orderInfo.create_date = dateTimeArr(0)
      //取到小时
      val hourStr: String = dateTimeArr(1).split(":")(0)
      orderInfo.create_hour = hourStr
      //敏感字段脱敏
      val tuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
      orderInfo.consignee_tel = tuple._1 + "*******"

      orderInfo
    }

    //TODO 转化样例类OrderDetail
    val realOrderDetailDStream: DStream[OrderDetail] = orderDetailDStream.map { record =>
      val str = record.value()
      val orderDetail: OrderDetail = JSON.parseObject(str, classOf[OrderDetail])
      //TODO ***********************************************
      //println("从表表！！！！"+orderDetail)
      orderDetail
    }
    // TODO 双流join 前 要把流变为kv 结构
    val orderInfoWithKeyDStream: DStream[(String, OrderInfo)] =
      orderInfoDStream.map(orderInfo => (orderInfo.id, orderInfo))
    val realOrderDetailWithKeyDStream: DStream[(String, OrderDetail)] =
      realOrderDetailDStream.map(orderDetail => (orderDetail.order_id, orderDetail))

    //TODO 为了不管是否能够关联左右 ，都要保留左右两边的数据 采用full join
    val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] =
      orderInfoWithKeyDStream.fullOuterJoin(realOrderDetailWithKeyDStream)


    val saleDetailDstream: DStream[SaleDetail] = fullJoinDStream.flatMap {
      case (orderInfo, (orderInfoOpt, orderDetailOpt)) =>

        //redis 客户端
        val redis = new Jedis("hadoop102", 6379)
        //关联结果的集合
        val saleDetailList= ListBuffer[SaleDetail]()
        //TODO 1. 如果orderInfo不为null
        if (orderInfoOpt != None) {
          val orderInfo: OrderInfo = orderInfoOpt.get
          //TODO ***********************************************
          //println("主表:"+orderInfo)
          if (orderDetailOpt != None) { //TODO 1.如果从表也不为none 关联从表
            val orderDetail: OrderDetail = orderDetailOpt.get
            //TODO ***********************************************
            // println("从表:"+orderDetail)
            //合并成一个宽表对象 ->SaleDetail
            val saleDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saleDetail
          }

          //val orderInfoJSON: String = JSON.toJSONString(orderInfo)  fastJson解析case class 不适用
          //使用json4s解析 case class
          implicit val formats = org.json4s.DefaultFormats
          //orderInfo对象转化为json字符串
          val orderInfoJson: String = Serialization.write(orderInfo)
          //TODO ***********************************************
          //println(orderInfoJson)

          //TODO 2  OrderInfo字符串存入redis (主表)    //TODO 2.把自己写入缓存
          // type  :String     key : order_info:[order_id]     value:order_info_json
          val orderInfoKey = "order_info:" + orderInfo.id
          redis.setex(orderInfoKey, 3600, orderInfoJson)

          //TODO 3 查询缓存中是否有对应的OrderDetai  (从表)      //TODO 3.查询缓存
          //type : Set    key:order_detail:order_id      value: 多个 order_tail json
          val orderDetailKey = "order_detail:" + orderInfo.id
          //从缓存中查询有没有与orderInfo.id 相同的orderDetail集合
          val orderDetailSet: util.Set[String] = redis.smembers(orderDetailKey)
          import scala.collection.JavaConversions._
          //遍历集合 与 orderInfo关联
          for (orderDetailJson <- orderDetailSet) {
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
            //写入集合
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saleDetail
          }
        } else if (orderDetailOpt != None) {
          // 如果orderDetail 从表不为null
          // TODO 1. 把自己写入缓存
          //orderDetail  type : Set key order_detail:order_id  ,value order_detail_json
          val orderDetail: OrderDetail = orderDetailOpt.get
          implicit val formats = org.json4s.DefaultFormats
          val orderDetailJson: String = Serialization.write(orderDetail)
          val orderDetailKey = "order_detail:" + orderDetail.order_id

          redis.sadd(orderDetailKey, orderDetailJson)
          //设置过期时间
          redis.expire(orderDetailKey, 3600)

          //2.查询缓存中是否有对应的OrderInfo
          val orderInfoKey: String = "order_info:" + orderDetail.order_id
          //主表只能关联一个,所以用get
          val orderInfoJson: String = redis.get(orderInfoKey)
          //如果查询到了OrderInfo,关联成saleDetail 放入集合
          if (orderInfoJson != null && orderDetailJson.size > 0) {
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            val saleDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saleDetail
          }
        }
        redis.close()
        saleDetailList
    }
//    saleDetailDstream.foreachRDD { rdd =>
//      println(rdd.collect().mkString("\n"))
//    }

    saleDetailDstream.mapPartitions{saleDetailItr=>
      val jedis = new Jedis("hadoop102", 6379)
      val saleDetailList: ListBuffer[SaleDetail] = new ListBuffer[SaleDetail]
      for (saleDetail <- saleDetailItr) {
        saleDetail
      }
      jedis
      saleDetailList.toIterator
    }

        //TODO User新增变化数据写入reids缓存
        userRecordDStream.map(record=>JSON.parseObject(record.value(), classOf[UserInfo])).
          foreachRDD{ rdd=>
          //用户信息的缓存  String  优点:数据量大,适合做分布式,key分布在不同的插槽中 缺点: 操作不方便(取出用户),可以单独控制用户过期时间
          //              Hash    优点:操作方便,所有用户在一个key中保存
          //type   key= user_info:[user_id]  v=  user_info
          rdd.foreachPartition{ userInfoItr=>
            implicit  val formats = org.json4s.DefaultFormats
            val jedis: Jedis = new Jedis("hadoop102", 6379)
            for(userInfo <- userInfoItr){
              val userInfoKey: String = "user_info:"+userInfo.id
              val userInfoJson:String = Serialization.write(userInfo)
              jedis.set(userInfoKey,userInfoJson)
            }
          }
        }
        //TODO 反查缓存关联userInfo   补充SaleDetail
        val userInfo_SaleDetailDStream: DStream[SaleDetail] = saleDetailDstream.mapPartitions { salItr =>
          val jedis: Jedis = new Jedis("hadoop102", 6379)
          //放置UserInfo关联saleDetail后  的对象集合
          val userList: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()

          for (saleDetail <- salItr) {
            //从缓存中查询
            val userInfoJson: String = jedis.get("user_info:" + saleDetail.user_id)
            if(userInfoJson!=null) {
              val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
              saleDetail.mergeUserInfo(userInfo) //合并数据
            }
            userList += saleDetail
          }
          jedis.close()
          userList.toIterator
        }


//        userInfo_SaleDetailDStream.foreachRDD{rdd=>
//          println(rdd.collect().mkString("\n"))
//        }


    //存入es
    userInfo_SaleDetailDStream.foreachRDD{rdd=>
      rdd.foreachPartition{saleDetailItr=>
        val dataList = saleDetailItr.map(saleDetail=>(saleDetail.order_detail_id,saleDetail)).toList
        MyESUtil.indexBulk(GmallConstants.ES_INDEX_SALE,dataList)
      }
    }


    ssc.start()
    ssc.awaitTermination()
  }
}
