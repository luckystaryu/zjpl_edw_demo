package com.zjpl.zjpl_edw_demo.dm

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object dm_search_stat_gov {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.debug.maxToStringFields", 800)
      .enableHiveSupport()
      .getOrCreate()
    var yest_dt: String = "1990-09-09"
    var dateForamt: SimpleDateFormat = null
    var yest_dt02: String = "1990-09-09"
    if (args.length != 0) {
      yest_dt = args(0)
      dateForamt = new SimpleDateFormat("yyyy-MM-dd")
      val yest_dt01: Date = dateForamt.parse(yest_dt)
      val cal = Calendar.getInstance()
      cal.setTime(yest_dt01)
      cal.add(Calendar.DATE, -1)
      yest_dt02 = dateForamt.format(cal.getTime)
    } else {
      val date: Date = new Date()
      dateForamt = new SimpleDateFormat("yyyy-MM-dd")
      val dt_date = dateForamt.format(date)
      val cal = Calendar.getInstance()
      cal.add(Calendar.DATE, -1)
      yest_dt = dateForamt.format(cal.getTime)
      cal.add(Calendar.DATE, -1)
      yest_dt02 = dateForamt.format(cal.getTime)
    }
    import spark.sql
    import spark.implicits._
    sql(
      s"""
         | create table if not exists dm_db.dm_search_stat_gov
         | ( id  string comment '序号'
         |   ,memberId string comment '用户ID'
         |   ,keyword  string comment '关键字'
         |   ,action_time timestamp comment '搜索时间'
         |   ,stdname     string comment '标准名称'
         |   ,province    string comment '省份'
         |   ,title_month string comment '月份'
         |   ,cid         string comment '一级分类代码'
         |   ,subcid      string comment '二级分类代码'
         |   ,code        string comment '材料编码'
         |   ,amount      string comment '搜索次数'
         |   ,sort_id     Integer comment '排序'
         | )partitioned by (etl_dt string)
         | stored as parquet
       """.stripMargin)
    sql("alter table dm_db.dm_search_stat_gov drop if exists partition (etl_dt='" + yest_dt + "')")
    val dm_search_stat_govDF=sql(
      s"""
         |insert into dm_db.dm_search_stat_gov partition(etl_dt='$yest_dt')
         |select tt.id
         |   ,tt.memberId
         |   ,tt.keyword
         |   ,tt.action_time
         |   ,tt.stdname
         |   ,tt.province
         |   ,tt.title_month
         |   ,tt.cid
         |   ,tt.subcid
         |   ,tt.code
         |   ,tt.amount
         |   ,tt.sort_id
         | from (
         | select tt1.id
         |   ,tt1.memberId
         |   ,tt1.keyword
         |   ,tt1.action_time
         |   ,tt1.stdname
         |   ,tt1.province
         |   ,tt1.title_month
         |   ,tt1.cid
         |   ,tt1.subcid
         |   ,tt1.code
         |   ,tt1.amount
         |   ,dense_rank()over(partition by tt1.stdname,tt1.province order by tt1.amount desc) as sort_id
         |from (
         |select t1.id
         |   ,t1.memberId
         |   ,t1.keyword
         |   ,t1.action_time
         |   ,t1.stdname
         |   ,t1.province
         |   ,t1.title_month
         |   ,t1.cid
         |   ,t1.subcid
         |   ,t1.code
         |   ,count(1)over(partition by t1.stdname,t1.province) as amount
         |  from dm_db.dm_search_record t1
         |  where to_date(t1.action_time)>=to_date('2018-01-01')
         |    and t1.price_type='gov') tt1
         |  )tt
         |  where tt.sort_id <=100
       """.stripMargin)
    spark.stop()
  }
}
