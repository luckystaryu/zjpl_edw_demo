package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object o_stg_mainfac {
  def main(args: Array[String]): Unit = {
    val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    var yest_dt: String = "1990-09-09"
    if (args.length !=0)
    {
      yest_dt = args(0)
    }else
    {
      val date:Date = new Date()
      val dateForamt:SimpleDateFormat = new  SimpleDateFormat("yyyy-MM-dd")
      val dt_date = dateForamt.format(date)
      val cal = Calendar.getInstance()
      cal.add(Calendar.DATE,-1)
      yest_dt =dateForamt.format(cal.getTime)
    }
    logger.info(yest_dt)
    val df = spark.read.format("json").load("/user/root/maidian/stg_mainfac_"+yest_dt+".log")
    logger.info("/user/root/maidian/o_stg_mainfac_"+yest_dt+".log")
    df.select("webId","type","labelName","referer","screenHeight","screenWidth","screenColorDepth","screenAvailHeight","screenAvailWidth",
    "title","domain","url","browserLang","browseAgent","browser","cookieEnabled","system","systemVersion","sessionId","ip","createTime")
      .write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("ods_db.o_stg_mainfac")
    spark.stop()
  }
}
