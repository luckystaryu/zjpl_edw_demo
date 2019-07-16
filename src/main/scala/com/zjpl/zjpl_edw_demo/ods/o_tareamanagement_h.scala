package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object o_tareamanagement_h {
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
         |create table if not exists ods_db.o_tareamanagement_h(
         | id          int    comment 'id'
         | ,regionCode string comment '行政区划id'
         | ,pid        int    comment '父类id'
         | ,name       string comment '行政区划名称'
         | ,renPing    string comment ''
         | ,isLeaf     int    comment '是否叶子节点'
         | ,isGov      int    comment '是否有信息价'
         | ,isOpen     int    comment ''
         | ,createOn   timestamp comment '创建时间'
         | ,isFac      int     comment '是否市场价'
         | ,industrys  string  comment '行业id'
         |)stored as parquet
       """.stripMargin)
    sql(
      s"""
         |insert  overwrite  table ods_db.o_tareamanagement_h
         |select id
         |,regionCode
         |,pid
         |,name
         |,renPing
         |,isLeaf
         |,isGov
         |,isOpen
         |,createOn
         |,isFac
         |,industrys
         |  from ods_db.o_tareamanagement
       """.stripMargin)
  }
}
