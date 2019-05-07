package com.zjpl.zjpl_edw_demo.sparkstreaming.structuredStreaming

import org.apache.spark.sql.SparkSession

/**
  * 先netcat -l -p 9998
  * spark2-submit \
--class com.zjpl.zjpl_edw_demo.sparkstreaming.structuredStreaming.StructuredNetworkWordCount \
--master yarn \
--num-executors 3 \
--executor-cores 24 \
--executor-memory 24G \
/data/zjpl/yxq/zjpl_edw_demo-1.0-SNAPSHOT.jar 172.16.1.62 9998
  */
object StructuredNetworkWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length <2){
      System.err.println("Usage:StructuredNetWordCount <hostname> <port>")
      System.exit(1)
    }
    val host = args(0)
    val port = args(1).toInt
    val spark =SparkSession
      .builder()
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._
    //Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
        .format("socket")
        .option("host",host)
        .option("port",port)
        .load()

    //Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    //Generate runnin word count
    val wordCounts = words.groupBy("value").count()

    //Start running the query that prints the running counts to the console

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
