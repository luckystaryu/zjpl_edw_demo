package com.zjpl.zjpl_edw_demo.sparkstreaming

import java.util

import com.alibaba.fastjson.JSON
import com.google.common.hash.Hashing
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.slf4j.{Logger, LoggerFactory}

object Kafka_to_HBase {
  val logger:Logger =LoggerFactory.getLogger(Kafka_to_HBase.getClass)

  def generateUniqueId(time: Long, partitionId: Int): Long ={
    Hashing.md5().hashString(time+""+partitionId).asLong()
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Kafka_to_HBase")
      .setMaster("local")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.streaming.kafka.maxRatePartition","100000")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "172.16.1.61:9092,172.16.1.62:9092,172.16.1.63:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" ->"aaaaa",
      //"auto.offset.reset" -> "earliest",
      "auto.offset.reset" ->"latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topicSet ="heyang".split(",").toSet
    val sc =new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(10))
    ssc.checkpoint("hdfs://master:8020/user/checkpoint")
    var puts = new util.ArrayList[Put]
    val POLICY_CREDDstream= KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topicSet,kafkaParams)
    )
    POLICY_CREDDstream.print()
    POLICY_CREDDstream.map(record =>JSON.parseObject(record.value()))
        .map(json => (json.getJSONObject("data"),json.getString("type")))
      .filter(data=>{
        if(data._2=="insert"||data._2=="update")
          true
        else
          false
      }).map(data => data._1)
        .foreachRDD((messages,time) => {
      val startTime = System.currentTimeMillis()
      if (!messages.isEmpty()) {
        messages.foreachPartition(partitionRDD=>{
          val partitionId = TaskContext.get.partitionId()
          val uniqueId = generateUniqueId(time.milliseconds, partitionId)
          val config = HBaseConfiguration.create()
          config.set("hbase.zookeeper.quorum", "172.16.1.61")
          config.set("hbase.zookeeper.property.clientPort", "2181")
          val conn = ConnectionFactory.createConnection(config)
          val table: Table = conn.getTable(TableName.valueOf("POLICY_CRED"))

          val jsondata = partitionRDD.foreach(data => {
            val rowkey = data.getString("p_num").reverse + "_" + data.getString("policy_status")+ "_"+ data.getBigDecimal("mor_rate").toString
            val put = new Put(rowkey.getBytes())
            val columns = data.keySet().toArray()
            columns.foreach(
              key =>{
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(key.toString), Bytes.toBytes(data.getString(key.toString)))
            })
            puts.add(put)
            println("测试结果============================"+puts.toString)
          })
          table.put(puts)
          table.close()
        })
        val endTime = System.currentTimeMillis()
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
