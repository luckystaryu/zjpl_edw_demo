package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object p_mysteel_data {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    import spark.sql
    sql(
      s"""
         |create table if not exists pdw_db.p_mysteel_data(
         |  name string comment '名称'
         | ,texture string comment '材质'
         | ,specification string comment '品牌'
         | ,city string comment '城市'
         | ,count string comment '数量'
         | ,price decimal(14,4) comment '价格'
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
    sql("alter table pdw_db.p_mysteel_data drop if exists partition (etl_dt ='" + yest_dt + "')")
    sql(
      s"""
         |insert into table pdw_db.p_mysteel_data partition(etl_dt ='$yest_dt')
         |select name
         |      ,texture
         |      ,specification
         |      ,city
         |      ,count
         |      ,cast(case when trim(price) is null or price='NULL'
         |                 then 0
         |                 else price
         |             end as decimal(14,4)) as price
         |      ,warehouse
         |      ,plant
         |      ,supplier
         |      ,contact
         |      ,telephone
         |      ,phone
         |      ,time
         |  from ods_db.o_mysteel_data_h
         | where to_date(etl_dt) = to_date('$yest_dt')
         |   and price rlike '^[0-9]*'
         |   and price is not null
       """.stripMargin)
    spark.stop()
  }
}
