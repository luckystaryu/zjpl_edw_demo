package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object a_data_package_report {
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
         |create table if not exists app_db.a_data_package_report
         |(webid     string comment '页面ID',
         | labelName string comment '标签名称',
         | labelname_url_name string comment '标签位置',
         | source_type_cd int comment '来源类型:1 web端,2 手机端',
         | pv_amount string comment 'PV数据量',
         | stoptime    int  comment '页面停留时间(秒)',
         | orders_amount  int comment '订单数量',
         | orders_amt     decimal(14) comment '订单金额'
         |)partitioned by (etl_dt string)
         |stored as parquet
       """.stripMargin)
    sql("alter table app_db.a_data_package_report drop if exists partition (etl_dt='" + yest_dt + "')")
    sql(
      s"""
         |insert into table app_db.a_data_package_report partition (etl_dt='$yest_dt')
         |select t1.webid
         |      ,t1.labelName
         |      ,t1.labelname_url_name
         |      ,t1.source_type_cd
         |      ,nvl(t2.pv_amount,0) as pv_amount
         |      ,nvl(t2.stoptime,0) as stoptime
         |      ,nvl(t3.orders_amount,0) as orders_amount
         |      ,nvl(t3.orders_amt,0) as orders_amt
         |  from ods_db.o_package_id_event_tracking_map_h t1
         |  left join dm_db.dm_data_package_pv t2
         |    on t1.labelName = t2.labelName
         |   and t1.webid = t2.webid
         |   and to_date(t2.etl_dt) = to_date('$yest_dt')
         |  left join dm_db.dm_data_package_orders_sum t3
         |    on t1.labelName = t3.labelName
         |   and to_date(t3.etl_dt) = to_date('$yest_dt')
       """.stripMargin)
    spark.stop()
  }
}
