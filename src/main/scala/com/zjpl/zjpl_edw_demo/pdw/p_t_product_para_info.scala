package com.zjpl.zjpl_edw_demo.pdw

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object p_t_product_para_info {
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
         | create table if not exists pdw_db.p_t_product_para_info
         |(id                   int        comment '参数序号id'
         |,para_name            string     comment '参数名称'
         |,para_value           string     comment '参数值'
         |,product_num          string     comment '产品编码'
         |,create_time          timestamp  comment '创建时间'
         |,creator_name         string     comment '创建人'
         |,update_time          timestamp  comment '修改时间'
         |,updator_name         string     comment '修改人'
         |)partitioned by (etl_dt string)
         |stored as parquet
       """.stripMargin)
    sql("alter table pdw_db.p_t_product_para_info drop if exists partition (etl_dt='"+yest_dt02+"')")
    sql("alter table pdw_db.p_t_product_para_info drop if exists partition (etl_dt='"+yest_dt+"')")
    sql(
      s"""
         |insert into table pdw_db.p_t_product_para_info  partition (etl_dt='$yest_dt')
         |select t1.id
         |      ,t1.code
         |      ,split(t2.feature,':')[0]
         |      ,split(t2.feature,':')[1]
         |      ,t1.createOn
         |      ,t1.createBy
         |      ,t1.updateOn
         |      ,t1.updateBy
         | from ods_db.o_tfacmaterialbase_h t1
         | lateral view explode(split(features,';')) t2 as feature
         |where t1.etl_dt='$yest_dt'
       """.stripMargin)
    spark.stop()
    spark.close()
  }
}
