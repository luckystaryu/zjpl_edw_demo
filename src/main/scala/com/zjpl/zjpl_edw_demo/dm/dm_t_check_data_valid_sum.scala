package com.zjpl.zjpl_edw_demo.dm

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object dm_t_check_data_valid_sum {
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
         | create table if not exists dm_db.dm_t_check_data_valid_sum
         |( table_cn_name string comment '表中文名'
         | ,source_table  string comment '来源表'
         | ,error_id      string comment '异常编号'
         | ,error_description    string comment '异常详情'
         | ,amount        int    comment '异常数据量'
         |)partitioned by(etl_dt string)
         | stored as parquet
       """.stripMargin)
    sql("alter table dm_db.dm_t_check_data_valid_sum drop if exists partition (etl_dt='"+yest_dt02+"')")
    sql("alter table dm_db.dm_t_check_data_valid_sum drop if exists partition (etl_dt='"+yest_dt+"')")
    sql(
      s"""
         |insert into dm_db.dm_t_check_data_valid_sum partition (etl_dt='$yest_dt')
         |select t1.table_cn_name
         |        ,t1.source_table
         |        ,t1.error_id
         |        ,t2.error_description
         |        ,count(1)
         |  from dm_db.dm_t_check_data_valid t1
         |  left join ods_db.o_t_data_error_def t2
         |    on t1.error_id = t2.error_id
         | where etl_dt='$yest_dt'
         | group by t1.table_cn_name
         |        ,t1.source_table
         |        ,t1.error_id
         |        ,t2.error_description
       """.stripMargin)
    spark.stop()
    spark.close()
  }
}
