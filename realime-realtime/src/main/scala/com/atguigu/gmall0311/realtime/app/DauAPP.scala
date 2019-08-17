package com.atguigu.gmall0311.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import com.alibaba.fastjson.JSON
import com.atguigu.gmall0311.realtime.bean.Startuplog
import com.atguigu.gmall0311.realtime.util.MyKafkaUtil
import com.atguigu.realimeGmall0311.common.constant.GmallConstants
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._

//daily action user
object DauAPP {

  def main(args: Array[String]): Unit = {
    //处理日活
    //TODO 1 创建上下文环境对象 和Streaming
    val conf = new SparkConf().setAppName("Dau_app").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))
    //创建kafka连接对象   到sparkstreaming
    val inputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

    //打印
    //    inputStream.foreachRDD{
    //      rdd=> println(rdd.map(_.value()).collect().mkString("/n"))
    //    }

    //TODO 2 转换样例类
    val startupLogDstream: DStream[Startuplog] = inputStream.map { record =>

      val startupJson: String = record.value()
      val startuplog: Startuplog = JSON.parseObject(startupJson, classOf[Startuplog])


      //时间戳转化为标准格式
      val dateTimeStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startuplog.tm))
      //补充两个日期
      startuplog.logDate = dateTimeStr.split(" ")(0)
      startuplog.logHour = dateTimeStr.split(" ")(1)

      startuplog

    }


    //TODO 3 不同批次间去重
    val filterStream: DStream[Startuplog] = startupLogDstream.transform { rdd =>
      //Driver
      //利用清单进行过滤,去重
      println("过滤前："+rdd.count())
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val dauKey: String = "dau:" + new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      //redis中的清单取出来
      val dauSet: util.Set[String] = jedis.smembers(dauKey)
      //放入广播变量
      val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
      jedis.close()
      //过滤掉清单中已经存在的value
      val fliterRDD: RDD[Startuplog] = rdd.filter { startupLog => //executor
        !dauBC.value.contains(startupLog.mid)
      }
      println("过滤后： " + fliterRDD.count())
      fliterRDD
    }

    //TODO 4 一个批次内去重 : 按照key分组,每组取一个
    val groupStream: DStream[(String, Iterable[Startuplog])] = filterStream.map(startupLog=>(startupLog.mid,startupLog)).groupByKey()
    val realFilteredDStream: DStream[Startuplog] = groupStream.flatMap { case (mid, startupLogIter) => {
      startupLogIter.take(1)
    }
    }
    realFilteredDStream.cache()
    //TODO 5 mid 记录每个批次访问过的mid 形成一个清单
    //更新清单,存储到Redis
    realFilteredDStream.foreachRDD{ rdd=>
      //driver
      // redis 用什么类型存储 ①String 好分片  ②set 好管理 选用set  key dau:日期
      rdd.foreachPartition{startuplogIter=>
        val jedis: Jedis = new Jedis("hadoop102",6379)  //excutor端 每个分区执行一次
        //遍历每个批次的数据 放入Redis
        for (startupLog <- startuplogIter) {
          //key  =  dau: + 日志生产日期
          val dauKey = "dau:"+startupLog.logDate
          println(dauKey+"::::"+startupLog.mid)
          jedis.sadd(dauKey,startupLog.mid)
        }
        jedis.close()
      }

    }

    realFilteredDStream.foreachRDD{rdd=>
      //存到hbase-phoenix中
      rdd.saveToPhoenix("gmall0311_dau",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
    }



    println("启动流程")

    ssc.start()
    ssc.awaitTermination()
  }
}
