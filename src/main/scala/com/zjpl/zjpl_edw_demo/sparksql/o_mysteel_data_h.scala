package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object o_mysteel_data_h {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val warehouseLocation  = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql
    sql(
      s"""
         |create table if not exists ods_db.o_mysteel_data_h(
         |  name string comment '名称'
         | ,texture string comment '材质'
         | ,specification string comment '品牌'
         | ,city string comment '城市'
         | ,count string comment '数量'
         | ,price string comment '价格'
         | ,warehouse string comment '仓库'
         | ,plant string comment '工厂'
         | ,supplier string comment '供应商'
         | ,contact string comment '联系人'
         | ,telephone string comment '电话号码'
         | ,phone bigint comment '手机号码'
         | ,time string comment '时间'
         | )partitioned by (etl_dt string)
         | stored as parquet
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
    sql("alter table ods_db.o_mysteel_data_h drop if exists partition (etl_dt ='"+yest_dt+"')")
    sql(
      s"""
         |insert into table ods_db.o_mysteel_data_h partition (etl_dt='$yest_dt')
         |select name
         |      ,texture
         |      ,specification
         |      ,city
         |      ,count
         |      ,price
         |      ,warehouse
         |      ,plant
         |      ,supplier
         |      ,contact
         |      ,telephone
         |      ,phone
         |      ,time
         |  from ods_db.o_mysteel_data
         |where from_unixtime(cast(time as bigint),'yyyy-MM-dd')='$yest_dt'
         |  and trim(price) rlike '^[0-9]'
       """.stripMargin)
    spark.stop()
  }
}
