package com.atguigu.gmall0311.realtime.util

import java.util
import java.util.Objects


import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index}

import collection.JavaConversions._
object MyESUtil {

  private val ES_HOST = "http://192.168.5.102"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = null

  /**
    * 获取客户端
    * @return
    */
  def getClient:JestClient={
    if(factory==null){
      build
    }
    factory.getObject
  }

  /**
    * 关闭客户端
    * @param client
    */
  def close(client: JestClient): Unit = {
    if(!Objects.isNull(client)){
      try
        client.shutdownClient()
      catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }

  /**
    * 建立连接
    */
  private def build(): Unit ={

    factory = new JestClientFactory

    factory.setHttpClientConfig(new HttpClientConfig.
    Builder(ES_HOST+":"+ES_HTTP_PORT).multiThreaded(true).
      maxTotalConnection(20)  //连接总数
    .connTimeout(10000).readTimeout(10000).build)
  }

  //indexName
  //type
  //id
  //source  case class
  /**
    * batch  批量保存数据
    * @param indexName
    * @param dataList
    */
                    //索引名        //传过来的批次数据
  def indexBulk(indexName:String , dataList: List[(String,Any)]): Unit ={
     val jestClient: JestClient = getClient
      val bulkBuilder = new Bulk.Builder
      for ((id,data) <- dataList) {
        val index= new Index.Builder(data).index(indexName).`type`("_doc").id(id).build()
        bulkBuilder.addAction(index)
      }
      val bulk:Bulk = bulkBuilder.build()
      val items: util.List[BulkResult#BulkResultItem] = jestClient.execute(bulk).getFailedItems
      println("保存"+items.size+"条")
      close(jestClient)


  }



  def main(args: Array[String]): Unit = {
    val jestClient: JestClient = getClient
    val index= new Index.Builder(Customer0311("lisi",114.1)).index("customer0311").`type`("doc").id("2").build()

    jestClient.execute(index)
    close(jestClient)

  }
}


case class Customer0311(customer_name:String,customer_amount:Double)
