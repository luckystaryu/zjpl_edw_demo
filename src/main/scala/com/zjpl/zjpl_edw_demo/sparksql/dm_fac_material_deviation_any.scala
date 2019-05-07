package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object dm_fac_material_deviation_any {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val warehouseLocation  = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.debug.maxToStringFields",200)
      .enableHiveSupport()
      .getOrCreate()
    import spark.sql
    sql(
      s"""
         |create table if not exists dm_db.dm_fac_material_price_any
         |(
         |   code    string comment '材价编码'
         |   cid     string comment '一级分类id',
         |   subcid  string comment '二级分类id',
         |   stdname string comment '标准名称',
         |   features string comment '特征',
         |   unit     string comment '单位',
         |   max_pricem decimal(14,4) comment '最大价格',
         |   min_pricem decimal(14,4) comment '最小价格'
         |)partitioned by (etl_dt string)
         |stored as parquet
         """.stripMargin)
    var yest_dt: String = "1990-09-09"
    var dt_date01:String ="1990-09-09"
    var last_month_date:String ="1990-09-09"
    val dateForamt:SimpleDateFormat = new  SimpleDateFormat("yyyy-MM-dd")
    if (args.length !=0)
    {
      yest_dt = args(0)
      val yest_date=dateForamt.parse(yest_dt)
      val cal_parse =Calendar.getInstance()
      val cal_parse01 =Calendar.getInstance()
      cal_parse.setTime(yest_date)
      cal_parse.add(Calendar.DATE,-1)
      cal_parse01.setTime(yest_date)
      cal_parse01.add(Calendar.MONTH,-1)
      dt_date01=dateForamt.format(cal_parse.getTime)
      last_month_date=dateForamt.format(cal_parse01.getTime)
    }else
    {
      val cal = Calendar.getInstance()
      cal.setTime(new Date())
      cal.add(Calendar.DATE,-1)
      yest_dt =dateForamt.format(cal.getTime)
      cal.add(Calendar.DATE,-1)
      dt_date01 =dateForamt.format(cal.getTime)
      val last_month = Calendar.getInstance()
      last_month.add(Calendar.DATE,-1)
      last_month.add(Calendar.MONTH,-1)
      last_month_date = dateForamt.format(last_month.getTime)
    }
    sql("alter table dm_db.dm_fac_material_price_any drop if exists partition (etl_dt ='"+yest_dt+"')")
    val exec_sql=sql(
      s"""
         |insert into table dm_db.dm_fac_material_price_any partition (etl_dt='$yest_dt')
         |select cid
         |      ,subcid
         |      ,stdname
         |      ,features
         |      ,unit
         |      ,max(pricem) as max_pricem
         |      ,min(pricem) as min_pricem
         | from odm_db.o_tfacmaterialbase
         |where cid in ('17','18','19','23','28','55','32')
         |  and isdeleted ='0'
         |  and isaudit = '1'
         |  and sourceType !='S'
         |group by cid
         |         ,subcid
         |         ,stdname
         |         ,features
         |         ,unit
         """.stripMargin)
    logger.warn("exec_sql"+exec_sql)
    spark.stop()
  }
}
