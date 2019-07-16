package com.zjpl.zjpl_edw_demo.dm

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object p_t_product_price_info_error {
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
         | create table if not exists dm_db.dm_t_check_data_valid
         |( table_cn_name string comment '表中文名'
         | ,source_table  string comment '来源表'
         | ,pk_name       string comment '主键'
         | ,source_pk_name string comment '来源主键'
         | ,pk_name_value   string comment '主键值'
         | ,error_id      string comment '异常编号'
         |)partitioned by(etl_dt string,table_name string)
         | stored as parquet
       """.stripMargin)
    sql("alter table dm_db.dm_t_check_data_valid drop if exists partition (etl_dt='"+yest_dt+"',table_name='p_t_product_price_info')")
    sql(
      s"""
         |insert into dm_db.dm_t_check_data_valid partition (etl_dt='$yest_dt',table_name='p_t_product_price_info')
         |select tt.table_cn_name
         |       ,tt.source_table
         |       ,tt.pk_name
         |       ,tt.source_pk_name
         |       ,tt.pk_name_value
         |       ,tt1.error_id as error_id
         |from (
         |    select '市场价基础表' as table_cn_name
         |            ,'tFacMaterialBase' as source_table
         |            ,'id' as pk_name
         |            ,'id'          as source_pk_name
         |            ,id   as pk_name_value
         |            ,concat_ws(',',case when length(t1.area_cd)!=char_length(area_cd)
         |                     then 'E0004'
         |                     else null
         |                 end) as error_id
         |           from pdw_db.p_t_product_price_info t1
         |          where length(t1.area_cd)!=char_length(area_cd) )tt
         |     LATERAL VIEW explode(split(error_id,',')) tt1 as error_id
       """.stripMargin)
    spark.stop()
  }
}
