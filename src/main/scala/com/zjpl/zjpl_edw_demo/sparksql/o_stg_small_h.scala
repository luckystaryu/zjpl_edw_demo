package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession

object o_stg_small_h {
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
         |Create table if not exists ods_db.o_stg_small_h(
         |   _webId string
         |  ,_f1 string
         |  ,_f2 string
         |  ,_f3 string
         |  ,_f4 string
         |  ,_f5 string
         |  ,_f6 string
         |  ,_f7 string
         |  ,_f8 string
         |  ,_f9 string
         |  ,_f10 string
         |  ,_f11 string
         |  ,_f12 string
         |  ,_f13 string
         |  ,_f14 string
         |  ,_f15 string
         |  ,_labelname string comment '标签'
         |  ,_corrupt_record string comment ''
         |  ,_type string comment '类型'
         |  ,_createtime string comment '创建时间'
         |  ,webId string comment '页面ID'
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
         |  ,userName  string comment '用户名称'
         |  ,sessionId string comment ''
         |  ,ip string comment ''
         |  ,createTime string comment ''
         |  ,stopTime string comment '在页面停留时间')
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
    sql("alter table ods_db.o_stg_small_h drop if exists partition (etl_dt='"+yest_dt+"')")
    sql(
      s"""
         |insert into ods_db.o_stg_small_h partition (etl_dt='$yest_dt')
         |select _webId
         |  ,_f1
         |  ,_f2
         |  ,_f3
         |  ,_f4
         |  ,_f5
         |  ,_f6
         |  ,_f7
         |  ,_f8
         |  ,_f9
         |  ,_f10
         |  ,_f11
         |  ,_f12
         |  ,_f13
         |  ,_f14
         |  ,_f15
         |  ,_labelname
         |  ,_type
         |  ,_corrupt_record
         |  ,_createtime
         |  ,webId
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
         |  ,userName
         |  ,sessionId
         |  ,ip
         |  ,createTime
         |  ,stopTime
         |from ods_db.o_stg_small
       """.stripMargin )
    spark.stop()
  }
}
