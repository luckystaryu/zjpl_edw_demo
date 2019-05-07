package com.zjpl.zjpl_edw_demo.sparkstreaming



import java.util.Date

import com.zjpl.zjpl_edw_demo.sparksql.Dao.maiDianDao
import com.zjpl.zjpl_edw_demo.sparksql.config.edw_config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.{Logger, LoggerFactory}

object o_stg_mobile {
   val logger:Logger =LoggerFactory.getLogger(o_stg_mobile.getClass)
   val zookeeperservers = edw_config.properties.getProperty("dev_zookeeper.server")
   val zookeeperport =edw_config.properties.getProperty("dev_zookeeper.port")
   val hbasePoolSize =edw_config.properties.getProperty("hbase.client.ipc.pool.size")
   var tablename="ODS_DB.O_STG_MOBILE"

   //HBASE 相关信息
   val HbaseConf:Configuration =HBaseConfiguration.create()
   HbaseConf.set("hbase.zookeeper.quorum",zookeeperservers)
   HbaseConf.set("hbase.zookeeper.port",zookeeperport)
   HbaseConf.set("hbase.client.ipc.pool.size",hbasePoolSize)

  var table:Table=_
  //定义case 类来解析json数据
//  case class o_stg_mobile(webid:String, `type`:String, labelname:String,referer:String,screenheight:String, screenwidth:String,
//                          screencolordepth:String, screenavailheight:String, screenavailwidth:String,title:String, domain:String,
//                          url:String,browserlang:String,browseagent:String,browser:String,cookieenabled:String, msystems:String,
//                          msystemversion:String,musername:String,msessionid:String,mip:String,mcreatetime:String)

  def main(args: Array[String]): Unit = {
    if (args.length <2){
      System.err.println(
        s"""
           |Usage:DirectKafkaWordCount <brokers> <topics>
           | <brokers> is a list of one or more Kafka brokers
           | <topics> is a list of one or more Kafka topics to consume from
         """.stripMargin)
      System.exit(1)
    }
    val Array(brokers,topics) =args
    val sparkConf = new SparkConf()
      .setAppName("o_stg_mobile")
      .set("spark.hbase.host","master,datanode01,datanode02")

    val ssc =new StreamingContext(sparkConf,Seconds(5))
    ssc.checkpoint("hdfs:/user/spark/streaming/checkpoint/stg_mobile")
    //TODO
    //1.创建kafka参数
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" ->brokers,
      "key.deserializer" ->classOf[StringDeserializer],
      "value.deserializer" ->classOf[StringDeserializer],
      "group.id" ->"use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" ->(false:java.lang.Boolean)
    )
    val topicsSet = topics.split(",").toSet
    val messages = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topicsSet,kafkaParams)
    )
    messages.map(record => record.value)
        .map(value =>{
          //隐式转换，使用json4s的默认转化器
          implicit val formats:DefaultFormats.type = DefaultFormats
          val json = parse(value)
          //样式类从json类提取
          json.extract[maiDianDao]
        }).foreachRDD(
      rdd =>{
        rdd.foreachPartition(partitionOfRecords=>{//循环分区
          //获取Hbase连接,分区创建一个连接,分区不跨节点，不需要要序列化
          val connection:Connection =ConnectionFactory.createConnection(HbaseConf)
          table=connection.getTable(TableName.valueOf(tablename))
          try{
            partitionOfRecords.foreach(
              stg_mobiledata =>{
                val tableput = new Put(Bytes.toBytes(String.valueOf(new Date().getTime) +"_"+stg_mobiledata.webId+"_"+stg_mobiledata.`type`))
                tableput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("webId"),Bytes.toBytes(stg_mobiledata.webId.toString))
                tableput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("type"),Bytes.toBytes(stg_mobiledata.`type`.toString))
                tableput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("browseAgent"),Bytes.toBytes(stg_mobiledata.browseAgent.toString))
                tableput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("browser"),Bytes.toBytes(stg_mobiledata.browser.toString))
                tableput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("browserLang"),Bytes.toBytes(stg_mobiledata.browserLang.toString))
                tableput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("cookieEnabled"),Bytes.toBytes(stg_mobiledata.cookieEnabled.toString))
                tableput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("createTime"),Bytes.toBytes(stg_mobiledata.createTime.toString))
                tableput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("domain"),Bytes.toBytes(stg_mobiledata.domain.toString))
                tableput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ip"),Bytes.toBytes(stg_mobiledata.ip.toString))
                tableput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("labelName"),Bytes.toBytes(stg_mobiledata.labelName.toString))
                tableput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("referer"),Bytes.toBytes(stg_mobiledata.referer.toString))
                tableput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("screenAvailHeight"),Bytes.toBytes(stg_mobiledata.screenAvailHeight.toString))
                tableput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("screenAvailWidth"),Bytes.toBytes(stg_mobiledata.screenAvailWidth.toString))
                tableput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("screenColorDepth"),Bytes.toBytes(stg_mobiledata.screenColorDepth.toString))
                tableput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("screenHeight"),Bytes.toBytes(stg_mobiledata.screenHeight.toString))
                tableput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("screenWidth"),Bytes.toBytes(stg_mobiledata.screenWidth.toString))
                tableput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("sessionId"),Bytes.toBytes(stg_mobiledata.sessionId.toString))
                tableput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("system"),Bytes.toBytes(stg_mobiledata.system.toString))
                tableput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("systemVersion"),Bytes.toBytes(stg_mobiledata.systemVersion.toString))
                tableput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("title"),Bytes.toBytes(stg_mobiledata.title.toString))
                tableput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("url"),Bytes.toBytes(stg_mobiledata.url.toString))
                tableput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("userName"),Bytes.toBytes(stg_mobiledata.userName.toString))
                table.put(tableput)
              }
            )
          }finally {
            table.close()
          }
        })
      }
    )

    val lines=messages.map(record => record.value)
      .map(value =>{
        //隐式转换，使用json4s的默认转化器
        implicit val formats:DefaultFormats.type = DefaultFormats
        val json = parse(value)
        //样式类从json类提取
        json.extract[maiDianDao]
      }).map(value=>(value.webId,1)).filter(x=>x._1!=null).reduceByKey(_+_)
    def mappingFunc=(inwebId:String,incount:Option[Int],state:State[Int])=>{
      val sum =incount.getOrElse(0) + state.getOption().getOrElse(0)
      val output=(inwebId,incount)
      state.update(sum)
      output
    }
    val stateStream =lines.mapWithState(StateSpec.function(mappingFunc))
    stateStream.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
