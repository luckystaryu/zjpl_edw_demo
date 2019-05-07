package com.zjpl.zjpl_edw_demo.sparkstreaming



import java.util.Date

import com.zjpl.zjpl_edw_demo.sparksql.config.edw_config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.{Logger, LoggerFactory}




object DirectKafkaWordCount {

  val logger:Logger = LoggerFactory.getLogger(this.getClass)

  val zookeeperservers =edw_config.properties.getProperty("dev_zookeeper.server")
  val zookeeperport = edw_config.properties.getProperty("dev_zookeeper.port")
  var tablenmae="dm_db.userHourly"
  //Hbase相关信息
  val HbaseConf:Configuration = HBaseConfiguration.create()
  HbaseConf.set("hbase.zookeeper.quorum",zookeeperservers)
  HbaseConf.set("hbase.zookeeper.property.clientPort",zookeeperport)

  var table:Table =_

  //定义case类来解析json数据
  case class apptimes(activetime:String,`package`:String)
  case class UserHourly(userId:String,endtime:Long,data:List[(String,Long)])
  case class log(userId:String,day:String,begintime:String,endtime:Long,data:List[apptimes])

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
    val Array(brokers,topics) = args
    //Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").set("spark.hbase.host","master,datanode01,datanode02")
    val ssc = new StreamingContext(sparkConf,Seconds(10))

    //Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" ->brokers,
      "key.deserializer" ->classOf[StringDeserializer],
      "value.deserializer" ->classOf[StringDeserializer],
      "group.id" ->"use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" ->(false:java.lang.Boolean)
    )
    /*
    *LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe
     */
    val topicsSet = topics.split(",").toSet
    val messages =KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topicsSet,kafkaParams)
    )
    messages.map(record =>(record.value))
      .map(value =>{
        //隐式转换，使用json4s的默认转化器
        implicit val formats:DefaultFormats.type = DefaultFormats
        val json = parse(value)
        //样式类从json对象提取值
        json.extract[log]
      }).window(Seconds(24*3600),Seconds(60)) //设置窗口时间，这个为每分钟分析一次一天的内容
      .foreachRDD(
      rdd =>{
        rdd.foreachPartition(partitionOfRecords =>{//循环分区
          //获取Hbase连接,分区创建一个连接,分区不跨节点，不需要要序列化
          var connection:Connection = ConnectionFactory.createConnection(HbaseConf)
          table = connection.getTable(TableName.valueOf(tablenmae))
          partitionOfRecords.foreach(logData =>{
            val theput = new Put(Bytes.toBytes(String.valueOf(new Date().getTime) + "_" + logData.endtime))
            theput.addColumn(Bytes.toBytes("info"),Bytes.toBytes("userId"),Bytes.toBytes(logData.userId.toString))
            logData.data.foreach(
              appTime =>{
                theput.addColumn(Bytes.toBytes("info"),Bytes.toBytes(appTime.`package`.toString),Bytes.toBytes(appTime.activetime.toString))
              })
            table.put(theput)
            table.close()
          })
        })
      }
    )
//    val lines = messages.map(_.value)
//    val words = lines.flatMap(_.split(" "))
//    val wordCounts = words.map(x =>(x,1)).reduceByKey(_+_)
//    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
