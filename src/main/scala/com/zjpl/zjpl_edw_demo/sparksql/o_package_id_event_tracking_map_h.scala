package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File

import org.apache.spark.sql.SparkSession

object o_package_id_event_tracking_map_h {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir",warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    import spark.sql
    sql(
      s"""
         |Create table if not exists ods_db.o_package_id_event_tracking_map_h(
         |   package_id string comment '套餐ID'
         |  ,webId string comment '页面ID'
         |  ,labelName string comment '标签名称'
         |  ,labelname_url_name string comment '位置'
         |  ,source_type_cd int comment '数据来源1:PC 2:移动'
         |  ,pay_flag string comment '付款页面标识'
         |  ,create_time  timestamp comment '创建时间'
         |  ) stored as parquet
       """.stripMargin)
    sql(
      s"""
         |insert overwrite table ods_db.o_package_id_event_tracking_map_h
         |select package_id
         |      ,webId
         |      ,labelName
         |      ,labelname_url_name
         |      ,source_type_cd
         |      ,pay_flag
         |      ,create_time
         |from  ods_db.o_package_id_event_tracking_map
       """.stripMargin )
    spark.stop()
  }
}
