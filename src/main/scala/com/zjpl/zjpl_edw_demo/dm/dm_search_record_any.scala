package com.zjpl.zjpl_edw_demo.dm

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.zjpl.zjpl_edw_demo.sparksql.spark_udf.customUDF
import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object dm_search_record_any {
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
    spark.udf.register("convertProvinceUDF",customUDF.convertProvinceUDF(_:String))
    sql(
      s"""
         | create table if not exists dm_db.dm_search_record_any
         | (keyword                string    comment '用户搜索词'
         | ,stdName                string    comment '标准名称'
         | ,subid                  string    comment '材料二级编码'
         | ,subname                string    comment '料材二级名称'
         | ,cid                    string    comment '材料一级编码'
         | ,cname                  string    comment '材料一级名称'
         | ,profession_id          string    comment '专业ID'
         | ,profession_name        string    comment '专业名称'
         | ,province               string    comment '省份'
         | ,search_amount          bigint    comment '搜索量'
         | ,week_amount            bigint    comment '每周搜索量'
         | ,month_search_amount    bigint    comment '每月搜索量'
         | ,year_search_amount     bigint    comment '每年搜索量'
         |)partitioned by (etl_dt string)
         | stored as parquet
       """.stripMargin)
    sql("alter table dm_db.dm_search_record_any drop if exists partition (etl_dt ='"+yest_dt+"')")
    val dm_search_record_any_tmpDF=sql(
      s"""
         |select t1.keyword
         |      ,t1.stdName
         |      ,t1.province
         |      ,sum(t1.search_amount) as search_amount
         |      ,sum(t1.week_amount)  as week_amount
         |      ,sum(t1.month_search_amount) as month_search_amount
         |      ,sum(t1.year_search_amount) as year_search_amount
         |from (
         |select keyword
         |      ,stdName
         |      ,convertProvinceUDF(cast(split(split(regexp_replace(url,'//','/'),'/')[1],'\\\\.')[0] as String)) as province
         |      ,0       as  search_amount
         |      ,0       as week_amount
         |      ,0       as month_search_amount
         |      ,count(1) as year_search_amount
         | from ods_db.o_t_std_record_h
         |where to_date(action_time)>to_date('$last_year')
         |  and to_date(action_time)<=to_date('$yest_dt')
         |group by keyword
         |        ,stdName
         |        ,convertProvinceUDF(cast(split(split(regexp_replace(url,'//','/'),'/')[1],'\\\\.')[0] as String))
         |union all
         |select keyword
         |      ,stdName
         |      ,convertProvinceUDF(cast(split(split(regexp_replace(url,'//','/'),'/')[1],'\\\\.')[0] as String)) as province
         |      ,0       as  search_amount
         |      ,0       as week_amount
         |      ,count(1)       as month_search_amount
         |      ,0 as year_search_amount
         |  from ods_db.o_t_std_record_h
         |where to_date(action_time)>to_date('$last_month')
         |  and to_date(action_time)<=to_date('$yest_dt')
         |group by keyword
         |        ,stdName
         |        ,convertProvinceUDF(cast(split(split(regexp_replace(url,'//','/'),'/')[1],'\\\\.')[0] as String))
         |union all
         |select keyword
         |      ,stdName
         |      ,convertProvinceUDF(cast(split(split(regexp_replace(url,'//','/'),'/')[1],'\\\\.')[0] as String)) as province
         |      ,0       as  search_amount
         |      ,count(1)       as week_amount
         |      ,0       as month_search_amount
         |      ,0 as year_search_amount
         |  from ods_db.o_t_std_record_h
         |where to_date(action_time)>to_date('$last_week')
         |  and to_date(action_time)<=to_date('$yest_dt')
         |group by keyword
         |        ,stdName
         |        ,convertProvinceUDF(cast(split(split(regexp_replace(url,'//','/'),'/')[1],'\\\\.')[0] as String))
         |union all
         |select keyword
         |      ,stdName
         |      ,convertProvinceUDF(cast(split(split(regexp_replace(url,'//','/'),'/')[1],'\\\\.')[0] as String))
         |      ,count(1)        as  search_amount
         |      ,0      as week_amount
         |      ,0       as month_search_amount
         |      ,0 as year_search_amount
         |  from ods_db.o_t_std_record_h
         |where to_date(action_time)=to_date('$yest_dt')
         |group by keyword
         |        ,stdName
         |        ,convertProvinceUDF(cast(split(split(regexp_replace(url,'//','/'),'/')[1],'\\\\.')[0] as String))) t1
         |group by t1.keyword
         |        ,t1.stdName
         |        ,t1.province
       """.stripMargin)
    dm_search_record_any_tmpDF.createOrReplaceGlobalTempView("dm_search_record_any_tmp")
    sql(
      s"""
         |insert into table dm_db.dm_search_record_any partition(etl_dt='$yest_dt')
         |select t1.keyword
         |      ,t1.stdName
         |      ,t2.subcid  as subcid
         |      ,t3.name as  subname
         |      ,t4.code as  cid
         |      ,t4.name as  cname
         |      ,t5.code as  profession_id
         |      ,t5.name as  profession_name
         |      ,t1.province
         |      ,t1.search_amount
         |      ,t1.week_amount
         |      ,t1.month_search_amount
         |      ,t1.year_search_amount
         | from global_temp.dm_search_record_any_tmp t1
         | left join ods_db.o_t_stdname_info t2
         |   on t1.stdname = t2.stdname
         | left join ods_db.o_tRationLib t3
         |  on t2.subcid = t3.code
         |  left join ods_db.o_tRationLib t4
         |   on t3.pid = t4.code
         |  left join ods_db.o_tRationLib t5
         |    on t4.pid = t5.code
         |""".stripMargin)
    spark.stop()
    spark.close()
  }
}
