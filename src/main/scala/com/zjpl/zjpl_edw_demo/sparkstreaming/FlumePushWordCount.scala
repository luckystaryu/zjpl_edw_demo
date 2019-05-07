package com.zjpl.zjpl_edw_demo.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

@deprecated
object FlumePushWordCount {
  def main(args: Array[String]): Unit = {

    if (args.length!=2){
      System.err.println("Uage:FlushPushWordCount <hostname> <port>")
      System.exit(1)
    }
    val Array(hostname,port) = args
    val sparkConf = new SparkConf().setAppName("FlushPushWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(60))
    //TODO streaming 整合flume
    val flumeStream = FlumeUtils.createStream(ssc,hostname,port.toInt)
    flumeStream.map(x => new String(x.event.getBody.array()).trim)
      .flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
