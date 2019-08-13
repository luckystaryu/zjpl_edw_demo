package com.zjpl.zjpl_edw_demo.ods

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object o_tGovExtends_anhui_h {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
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
         |create table if not exists ods_db.o_tGovExtends_anhui_h
         |(id                                  int	              comment '自增长主键'
         |,pid                                 int	            comment '主表ID'
         |,title                               string           comment '信息价标题'
         |,tax_price                           decimal(14,4)    comment '含税价'
         |,tax_price_url                       string           comment '含税价图片'
         |,no_tax_price                        decimal(14,4)    comment '除税价'
         |,no_tax_price_url                    string           comment '除税价图片'
         |,comprehensive_discount_rate         decimal(14,4)    comment '综合折税率'
         |,value_added_tax_rate                decimal(14,4)    comment '增值税率'
         |,business_tax_model_price            decimal(14,4)    comment '营业税模式价格'
         |,business_tax_model_price_url        string           comment '营业税模式价格图片'
         |,value_added_tax_model_price         decimal(14,4)    comment '增值税模式价格'
         |,value_added_tax_model_price_url     string           comment '增值税模式价格图片'
         |)partitioned by (etl_dt string)
         | stored as parquet
       """.stripMargin)
    sql("alter table ods_db.o_tGovExtends_anhui_h drop if exists partition (etl_dt='"+yest_dt02+"')")
    sql("alter table ods_db.o_tGovExtends_anhui_h drop if exists partition (etl_dt='"+yest_dt+"')")
    sql(
      s"""
         |insert into table ods_db.o_tGovExtends_anhui_h partition (etl_dt='$yest_dt')
         |select  id
         |,pid
         |,title
         |,tax_price
         |,tax_price_url
         |,no_tax_price
         |,no_tax_price_url
         |,comprehensive_discount_rate
         |,value_added_tax_rate
         |,business_tax_model_price
         |,business_tax_model_price_url
         |,value_added_tax_model_price
         |,value_added_tax_model_price_url
         |  from  ods_db.o_tGovExtends_anhui
       """.stripMargin)
    spark.stop()
    spark.close()
  }
}
