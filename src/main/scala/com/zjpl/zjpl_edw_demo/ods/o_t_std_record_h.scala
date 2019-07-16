package com.zjpl.zjpl_edw_demo.ods

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object o_t_std_record_h {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    var yest_dt: String = "1990-09-09"
    if (args.length != 0) {
      yest_dt = args(0)
    } else {
      val date: Date = new Date()
      val dateForamt: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val dt_date = dateForamt.format(date)
      val cal = Calendar.getInstance()
      cal.add(Calendar.DATE, -1)
      yest_dt = dateForamt.format(cal.getTime)
    }
    import spark.sql
    sql(
      s"""
         |Create table if not exists ods_db.o_t_std_record_h
         |(id              bigint    comment '序号'
         |,memberId        string    comment '用户的memberId'
         |,ip              string    comment '访问来源ip'
         |,keyword         string    comment '用户搜索词'
         |,action_time     timestamp comment '搜索时间'
         |,stdName         string    comment '标准化名称'
         |,province        string    comment '省份'
         |,title           string    comment '页面title'
         |,url             string    comment '网页URL'
         |,isResult        int       comment '搜索结果0.无结果  1.有结果'
         |,status          int       comment '1.页面顶部搜索  2.页面中间搜索  3.其他'
         |)partitioned by (etl_dt string)
         | stored as parquet
       """.stripMargin)
    sql("alter table ods_db.o_t_std_record_h drop if exists partition (etl_dt ='"+yest_dt+"')")
    sql(
      s"""
         |insert into table ods_db.o_t_std_record_h partition(etl_dt='$yest_dt')
         |select id
         |,memberId
         |,ip
         |,keyword
         |,action_time
         |,stdName
         |,province
         |,title
         |,url
         |,isResult
         |,status
         | from ods_db.o_t_std_record
       """.stripMargin)
    spark.stop()
    spark.close()
  }
}
