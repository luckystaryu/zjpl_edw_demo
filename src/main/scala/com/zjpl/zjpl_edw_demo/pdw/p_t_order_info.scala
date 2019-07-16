package com.zjpl.zjpl_edw_demo.pdw

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object p_t_order_info {
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
         | create table if not exists pdw_db.p_t_supplier_contactor_info
         |( id                 int
         | ,supplier_num       string     comment '供应商编号'
         | ,contactor_name     string     comment '联系人姓名'
         | ,sex_cd             string     comment '性别代码'
         | ,duty               string     comment '职务'
         | ,contact_type_cd    string     comment '联系方式类型代码'
         | ,contact_type       string     comment '联系方式'
         | ,create_time        timestamp  comment '创建时间'
         | ,creator_name       string     comment '创建人'
         | ,update_time        timestamp  comment '修改时间'
         | ,updator_name       string     comment '修改人'
         |)partitioned by(etl_dt string)
         | stored as parquet
       """.stripMargin)
    sql("alter table pdw_db.p_t_supplier_contactor_info drop if exists partition (etl_dt='"+yest_dt+"')")
    sql(
      s"""
         |insert into pdw_db.p_t_supplier_contactor_info partition (etl_dt='$yest_dt')

       """.stripMargin)
  }
}
