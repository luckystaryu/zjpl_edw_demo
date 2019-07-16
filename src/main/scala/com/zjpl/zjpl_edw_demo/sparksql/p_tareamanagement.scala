package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object p_tareamanagement {
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
         |create table if not exists pdw_db.p_tareamanagement(
         | id          int    comment 'id'
         | ,regionCode string comment '行政区划id'
         | ,name       string comment '行政区划名称'
         | ,regionCode_2 string comment '二级行政区划id'
         | ,name_2       string comment '二级行政区划名称'
         | ,regionCode_1 string comment '一级行政区划id'
         | ,name_1      string comment '一级行政区划名称'
         | ,pid        int    comment '父类id'
         | ,renPing    string comment ''
         | ,isLeaf     int    comment '是否叶子节点'
         | ,isGov      int    comment '是否有信息价'
         | ,isOpen     int    comment ''
         | ,createOn   timestamp comment '创建时间'
         | ,isFac      int     comment '是否市场价'
         | ,industrys  string  comment '行业id'
         | )stored as parquet
       """.stripMargin)
     sql(
       s"""
          |insert overwrite table pdw_db.p_tareamanagement
          |SELECT t1.id
          |     ,t1.regionCode
          |			,t1.name
          |			,coalesce(t2.regionCode,t1.regionCode) as regionCode_2
          |			,coalesce(t2.name,t1.name) as name_2
          |			,coalesce(t3.regionCode,t2.regionCode,t1.regionCode) as pregionCode_1
          |			,coalesce(t3.name,t2.name,t1.name) as name_1
          |     ,t1.pid
          |			,t1.renPing
          |			,t1.isLeaf
          |			,t1.isGov
          |			,t1.isOpen
          |			,t1.createOn
          |			,t1.isFac
          |			,t1.industrys
          |FROM ods_db.o_tAreaManagement_h t1
          |left join ods_db.o_tAreaManagement_h t2
          |  on t1.pid = t2.id
          |left join ods_db.o_tAreaManagement_h t3
          |  on t2.pid = t3.id
        """.stripMargin)
  }
}
