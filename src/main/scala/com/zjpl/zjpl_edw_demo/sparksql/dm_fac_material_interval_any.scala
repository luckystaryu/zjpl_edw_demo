package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object dm_fac_material_interval_any {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.debug.maxToStringFields", 200)
      .enableHiveSupport()
      .getOrCreate()
    import spark.sql
    sql(
      s"""
         |     create table if not exists dm_db.dm_fac_material_interval_any
         |     (cid      string comment '一级分类id',
         |      subcid   string comment '二级分类id',
         |      stdname  string comment '标准名称',
         |      features string comment '特征',
         |      unit     string comment '单位',
         |      max_pricem decimal(14,4) comment '最大价格',
         |      min_pricem decimal(14,4) comment '最小价格',
         |      interval_pricem decimal(14,4) comment '区间间距',
         |      one_interval_pricem decimal(14,4) comment '一级区间价格',
         |      one_interval_cnt int comment '一级区间数量',
         |      two_interval_pricem decimal(14,4) comment '二级区间价格',
         |      two_interval_cnt int comment '二级区间数量',
         |      three_interval_pricem decimal(14,4) comment '三级区间价格',
         |      three_interval_cnt int comment '三级区间数量',
         |      four_interval_pricem decimal(14,4) comment '四级区间价格',
         |      four_interval_cnt int comment '四级区间数量',
         |      five_interval_pricem decimal(14,4) comment '五级区间价格',
         |      five_interval_cnt int  comment '五级区间价格'
         |     )partitioned by (etl_dt string)
         |     stored as parquet
         """.stripMargin)
    var yest_dt: String = "1990-09-09"
    var dt_date01: String = "1990-09-09"
    var last_month_date: String = "1990-09-09"
    val dateForamt: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    if (args.length != 0) {
      yest_dt = args(0)
      val yest_date = dateForamt.parse(yest_dt)
      val cal_parse = Calendar.getInstance()
      val cal_parse01 = Calendar.getInstance()
      cal_parse.setTime(yest_date)
      cal_parse.add(Calendar.DATE, -1)
      cal_parse01.setTime(yest_date)
      cal_parse01.add(Calendar.MONTH, -1)
      dt_date01 = dateForamt.format(cal_parse.getTime)
      last_month_date = dateForamt.format(cal_parse01.getTime)
    } else {
      val cal = Calendar.getInstance()
      cal.setTime(new Date())
      cal.add(Calendar.DATE, -1)
      yest_dt = dateForamt.format(cal.getTime)
      cal.add(Calendar.DATE, -1)
      dt_date01 = dateForamt.format(cal.getTime)
      val last_month = Calendar.getInstance()
      last_month.add(Calendar.DATE, -1)
      last_month.add(Calendar.MONTH, -1)
      last_month_date = dateForamt.format(last_month.getTime)
    }
    sql("alter table dm_db.dm_fac_material_interval_any drop if exists partition (etl_dt ='" + yest_dt + "')")
    val exec_sql = sql(
      s"""
         |insert into table dm_db.dm_fac_material_interval_any partition (etl_dt='$yest_dt')
         |select t1.cid
         |      ,t1.subcid
         |      ,t1.stdname
         |      ,t1.features
         |      ,t1.unit
         |      ,max(t2.max_pricem) as max_pricem
         |      ,max(t2.min_pricem) as min_pricem
         |      ,max((t2.max_pricem-t2.min_pricem)/5) as interval_pricem
         |      ,max(t2.min_pricem+(t2.max_pricem-t2.min_pricem)/5) as one_interval_pricem
         |      ,sum(case when t1.pricem>=t2.min_pricem and t1.pricem <t2.min_pricem+(t2.max_pricem-t2.min_pricem)/5
         |                  then 1
         |                  else 0
         |             end) as one_interval_cnt
         |      ,max(t2.min_pricem+(t2.max_pricem-t2.min_pricem)*2/5) as two_interval_pricem
         |      ,sum(case when t1.pricem>=t2.min_pricem+(t2.max_pricem-t2.min_pricem)/5
         |                 and t1.pricem < t2.min_pricem+(t2.max_pricem-t2.min_pricem)*2/5
         |                 then 1
         |                 else 0
         |             end) as  two_interval_cnt
         |      ,max(t2.min_pricem+(t2.max_pricem-t2.min_pricem)*3/5) as three_interval_pricem
         |      ,sum(case when t1.pricem>=t2.min_pricem+(t2.max_pricem-t2.min_pricem)*2/5
         |                 and t1.pricem < t2.min_pricem+(t2.max_pricem-t2.min_pricem)*3/5
         |                then 1
         |                else 0
         |            end) as three_interval_cnt
         |      ,max(t2.min_pricem+(t2.max_pricem-t2.min_pricem)*4/5) as three_interval_pricem
         |      ,sum(case when t1.pricem>=t2.min_pricem+(t2.max_pricem-t2.min_pricem)*3/5
         |                 and t1.pricem < t2.min_pricem+(t2.max_pricem-t2.min_pricem)*4/5
         |                then 1
         |                else 0
         |            end) as four_interval_pricem
         |      ,max(t2.max_pricem) as five_interval_pricem
         |      ,sum(case when t1.pricem>=t2.min_pricem+(t2.max_pricem-t2.min_pricem)*4/5
         |                 and t1.pricem <=t2.max_pricem
         |                then 1
         |                else 0
         |            end) as five_interval_cnt
         | from odm_db.o_tfacmaterialbase t1
         | left join dm_db.dm_fac_material_price_any t2
         |   on t1.cid = t2.cid
         |  and t1.subcid = t2.subcid
         |  and t1.stdname = t2.stdname
         |  and t1.features = t2.features
         |  and t1.unit = t2.unit
         |where t1.cid in ('17','18','19','23','28','55','32')
         |  and t1.isdeleted ='0'
         |  and t1.isaudit = '1'
         |  and t1.sourceType !='S'
         |group by t1.cid
         |         ,t1.subcid
         |         ,t1.stdname
         |         ,t1.features
         |         ,t1.unit
         """.stripMargin)
    logger.warn("exec_sql" + exec_sql)
    spark.stop()
  }
}
