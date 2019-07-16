package com.zjpl.zjpl_edw_demo.dm

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object dm_search_stat_d {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    var yest_dt: String = "1990-09-09"
    val dateForamt: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    if (args.length != 0) {
      yest_dt = args(0)
    } else {
      val date: Date = new Date()
      val dt_date = dateForamt.format(date)
      val cal = Calendar.getInstance()
      cal.add(Calendar.DATE, -1)
      yest_dt = dateForamt.format(cal.getTime)
    }
    val yest_date=dateForamt.parse(yest_dt)
    val cal_last_year=Calendar.getInstance()
    val cal_last_month=Calendar.getInstance()
    val cal_last_week =Calendar.getInstance()
    cal_last_year.setTime(yest_date)
    cal_last_month.setTime(yest_date)
    cal_last_week.setTime(yest_date)
    cal_last_year.add(Calendar.YEAR,-1)
    cal_last_month.add(Calendar.MONTH,-1)
    cal_last_week.add(Calendar.DATE,-7)
    val last_year=dateForamt.format(cal_last_year.getTime)
    val last_month = dateForamt.format(cal_last_month.getTime)
    val last_week = dateForamt.format(cal_last_week.getTime)
    logger.info("last_year"+last_year)
    logger.info("last_month"+last_month)
    logger.info("last_week"+last_week)
    import spark.sql
    sql(
      s"""
         | create table if not exists dm_db.dm_search_stat_d
         | (keyword         string    comment '用户搜索词'
         | ,search_amount          bigint    comment '搜索量'
         | ,week_amount            bigint    comment '每周搜索量'
         | ,month_search_amount    bigint    comment '每月搜索量'
         | ,year_search_amount     bigint    comment '每年搜索量'
         |)partitioned by (etl_dt string)
         | stored as parquet
       """.stripMargin)
    sql("alter table dm_db.dm_search_stat_d drop if exists partition (etl_dt ='"+yest_dt+"')")
    sql(
      s"""
         |insert into dm_db.dm_search_stat_d partition(etl_dt = '$yest_dt')
         |select t1.keyword
         |      ,sum(t1.search_amount)
         |      ,sum(t1.week_amount)
         |      ,sum(t1.month_search_amount)
         |      ,sum(t1.year_search_amount)
         |from (
         |select keyword
         |      ,0       as  search_amount
         |      ,0       as week_amount
         |      ,0       as month_search_amount
         |      ,count(1) as year_search_amount
         | from ods_db.o_t_std_record_h
         |where to_date(action_time)>to_date('$last_year')
         |  and to_date(action_time)<=to_date('$yest_dt')
         |group by keyword
         |union all
         |select keyword
         |      ,0       as  search_amount
         |      ,0       as week_amount
         |      ,count(1)       as month_search_amount
         |      ,0 as year_search_amount
         |  from ods_db.o_t_std_record_h
         |where to_date(action_time)>to_date('$last_month')
         |  and to_date(action_time)<=to_date('$yest_dt')
         |group by keyword
         |union all
         |select keyword
         |      ,0       as  search_amount
         |      ,count(1)       as week_amount
         |      ,0       as month_search_amount
         |      ,0 as year_search_amount
         |  from ods_db.o_t_std_record_h
         |where to_date(action_time)>to_date('$last_week')
         |  and to_date(action_time)<=to_date('$yest_dt')
         |group by keyword
         |union all
         |select keyword
         |      ,count(1)        as  search_amount
         |      ,0      as week_amount
         |      ,0       as month_search_amount
         |      ,0 as year_search_amount
         |  from ods_db.o_t_std_record_h
         |where to_date(action_time)=to_date('$yest_dt')
         |group by keyword) t1
         |group by t1.keyword
       """.stripMargin)
    spark.stop()
    spark.close()
  }
}
