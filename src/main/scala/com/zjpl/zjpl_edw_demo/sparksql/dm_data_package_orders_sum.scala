package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object dm_data_package_orders_sum {
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
         |create table if not exists dm_db.dm_data_package_orders_sum
         |(etl_dt    string comment '数据日期',
         | labelName string comment '标签名称',
         | labelname_url_name string comment '标签位置',
         | source_type_cd int comment '来源类型:1 web端,2 手机端',
         | orders_amount  int comment '订单数量',
         | orders_amt     decimal(14) comment '订单金额'
         |)
         |stored as parquet
       """.stripMargin)
    sql(
      s"""
         |insert overwrite table dm_db.dm_data_package_orders_sum
         |select cast(to_date(t2.pay_time) as string)
         |      ,t3.labelName
         |      ,t3.labelname_url_name
         |      ,t3.source_type_cd
         |      ,count(1) as orders_amount
         |      ,sum(nvl(t1.price,0)) as orders_amt
         |  from ods_db.o_t_orders_items_h t1
         | inner join ods_db.o_t_orders_h t2
         |    on t1.order_id = t2.order_id
         |   and t2.order_status='1'
         | inner join ods_db.o_package_id_event_tracking_map_h t3
         |   on t1.package_id = t3.package_id
         |  and t2.order_source = t3.source_type_cd
         |  and t3.pay_flag='1'
         | where t1.package_name like '%数据包%'
         | group by t3.labelName
         |      ,t3.labelname_url_name
         |      ,t3.source_type_cd
         |      ,cast(to_date(t2.pay_time) as string)
       """.stripMargin)
    spark.stop()
  }
}
