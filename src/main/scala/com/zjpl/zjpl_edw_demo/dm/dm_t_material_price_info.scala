package com.zjpl.zjpl_edw_demo.dm

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object dm_t_material_price_info {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.debug.maxToStringFields",800)
      .enableHiveSupport()
      .getOrCreate()
    var yest_dt: String = "1990-09-09"
    var dateForamt: SimpleDateFormat =null
    var yest_dt02:String = "1990-09-09"
    if (args.length != 0) {
      yest_dt = args(0)
      dateForamt = new SimpleDateFormat("yyyy-MM-dd")
      val yest_dt01:Date =dateForamt.parse(yest_dt)
      val cal= Calendar.getInstance()
      cal.setTime(yest_dt01)
      cal.add(Calendar.DATE, -1)
      yest_dt02 = dateForamt.format(cal.getTime)
    } else {
      val date: Date = new Date()
      dateForamt= new SimpleDateFormat("yyyy-MM-dd")
      val dt_date = dateForamt.format(date)
      val cal = Calendar.getInstance()
      cal.add(Calendar.DATE, -1)
      yest_dt = dateForamt.format(cal.getTime)
      cal.add(Calendar.DATE, -1)
      yest_dt02 = dateForamt.format(cal.getTime)
    }
    import spark.sql
    sql(
      s"""
         | create table if not exists dm_db.dm_t_material_price_info
         | (code10 string comment '代码',
         | ,addr   string comment '地址',
         | ,name   string comment '名称',
         | ,spec   string comment '规格',
         | ,unit   string comment '单位',
         | ,brand  string comment '品牌'
         | ,tax_price decimal(14,4) comment '含税价'
         | ,no_tax_price decimal(14,4) comment '除税价'
         | )partitioned by(etl_dt string)
         | stored as parquet
       """.stripMargin)
    sql("alter table dm_db.dm_t_material_price_info drop if exists partition (etl_dt='"+yest_dt02+"')")
    sql("alter table dm_db.dm_t_material_price_info drop if exists partition (etl_dt='"+yest_dt+"')")
    sql(
      s"""
         |insert into dm_db.dm_t_material_price_info partition (etl_dt='$yest_dt')
       """.stripMargin)
    spark.stop()
    spark.close()
  }
}
