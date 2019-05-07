package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{SaveMode, SparkSession}

object ZJX_DATA {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    val date:Date = new Date()
    val dateForamt:SimpleDateFormat = new  SimpleDateFormat("yyyy-MM-dd")
    val dt_date = dateForamt.format(date)
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE,-1)
    val yest_dt =dateForamt.format(cal.getTime)
    val df = spark.read.format("csv").load("/data/zjpl/zjx_data/中价协工程计价信息网市场材料价格(福建201702)1.xlsx")
    df.select("webId","type","labelName","referer","screenHeight","screenWidth","screenColorDepth","screenAvailHeight","screenAvailWidth",
      "title","domain","url","browserLang","browseAgent","browser","cookieEnabled","system","systemVersion","sessionId","ip","createTime")
      .write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("ods_db.o_stg_main")
    spark.stop()
  }
}
