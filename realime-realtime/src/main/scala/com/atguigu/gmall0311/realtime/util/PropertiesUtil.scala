package com.atguigu.gmall0311.realtime.util

import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {

  def main(args: Array[String]): Unit = {
    val properties: Properties = PropertiesUtil.load("config.properties")

    println(properties.getProperty("kafka.broker.list"))
  }

  def load(propertieName:String): Properties ={
    val prop=new Properties();
    //getResourceAsStream --返回用于读取指定资源(propertieName)的输入流。
    prop.load(new InputStreamReader(Thread.currentThread().
      getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }

}

