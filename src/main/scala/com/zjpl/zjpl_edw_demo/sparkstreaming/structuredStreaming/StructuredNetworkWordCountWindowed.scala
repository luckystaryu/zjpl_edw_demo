package com.zjpl.zjpl_edw_demo.sparkstreaming.structuredStreaming

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StructuredNetworkWordCountWindowed {
  def main(args: Array[String]): Unit = {
    if(args.length < 3) {
      System.err.println("Usage:StructuredNetworkWordCountWindowed <hostname> <port>"
        + "<window duration in seconds> [<slide duration in secondes>]")
      System.exit(1)
    }
    val host = args(0)
    val port = args(1).toInt
    val windowSize = args(2).toInt
    val slideSize = if (args.length == 3)  windowSize else args(3).toInt
    if(slideSize > windowSize){
      System.err.println("<slide duration> must be less than or equal to <window duration>")
    }
    val windowDuration = s"$windowSize seconds"
    val slideDuration =s"$slideSize seconds"

    val spark = SparkSession
      .builder()
      .appName("StructuredNetworkWordCountWindowed")
      .getOrCreate()

    import spark.implicits._
    //Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host",host)
      .option("port",port)
      .option("includeTimestamp",true)
      .load()

    //Split the lines into words,retaining timestamps
    val words = lines.as[(String,Timestamp)]
      .flatMap(line =>line._1.split(" ")
        .map(word=>(word,line._2)))
      .toDF("word","timestamp")

    //Group the data window and word compute the count of each group
    val windowedCounts = words.groupBy(
      window($"timestamp",windowDuration,slideDuration),$"word"
    ).count().orderBy("window")

    //Start running the query that prints the windowed word counts to console
    val query = windowedCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate","false")
      .start()
    query.awaitTermination()
    }
}
