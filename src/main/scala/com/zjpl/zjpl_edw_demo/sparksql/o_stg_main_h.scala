package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession

object o_stg_main_h {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir",warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    import spark.sql
    sql(
      s"""
         |Create table if not exists ods_db.o_stg_main_h(
         |   webId string comment '页面ID'
         |  ,type string  comment '类型'
         |  ,labelName string comment '标签名称'
         |  ,referer string comment ''
         |  ,screenHeight string comment ''
         |  ,screenWidth string comment ''
         |  ,screenColorDepth string comment ''
         |  ,screenAvailHeight string comment ''
         |  ,screenAvailWidth string comment ''
         |  ,title string comment ''
         |  ,domain string comment ''
         |  ,url string comment ''
         |  ,browserLang string comment ''
         |  ,browseAgent string comment ''
         |  ,browser string comment ''
         |  ,cookieEnabled string comment ''
         |  ,system string comment''
         |  ,systemVersion string comment ''
         |  ,sessionId string comment ''
         |  ,ip string comment ''
         |  ,createTime string comment '')
         |  partitioned by(etl_dt string)
         |  stored as parquet
       """.stripMargin)
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
    sql("alter table ods_db.o_stg_main_h drop if exists partition (etl_dt='"+yest_dt+"')")
    sql(
      s"""
         |insert into ods_db.o_stg_main_h partition (etl_dt='$yest_dt')
         |select webId
         |  ,type
         |  ,labelName
         |  ,referer
         |  ,screenHeight
         |  ,screenWidth
         |  ,screenColorDepth
         |  ,screenAvailHeight
         |  ,screenAvailWidth
         |  ,title
         |  ,domain
         |  ,url
         |  ,browserLang
         |  ,browseAgent
         |  ,browser
         |  ,cookieEnabled
         |  ,system
         |  ,systemVersion
         |  ,sessionId
         |  ,ip
         |  ,createTime
         |from ods_db.o_stg_main
       """.stripMargin )
    spark.stop()
  }
}
