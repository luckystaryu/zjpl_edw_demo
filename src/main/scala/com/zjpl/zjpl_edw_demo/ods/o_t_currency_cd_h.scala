package com.zjpl.zjpl_edw_demo.ods

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object o_t_currency_cd_h {
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
         |create table if not exists ods_db.o_t_currency_cd_h(
         |  start_dt              string comment '开始时间'
         | ,end_dt                string comment '结束时间'
         | ,currency_cd           string comment '币种代码'
         | ,currency_cn_name      string comment '币种名称'
         | ,create_time           timestamp comment '创建时间'
         | ,creator_name          string    comment '创建人'
         | ,update_time           timestamp comment '修改时间'
         | ,updator_name          string    comment '修改人'
         |)stored as parquet
       """.stripMargin)

    /**
      * 1.将历史数据保留到临时表中
      */
    val o_t_currency_cd_h_tmp=sql(
      s"""
         |select t1.start_dt
         |,t1.end_dt
         |,t1.currency_cd
         |,t1.currency_cn_name
         |,t1.create_time
         |,t1.creator_name
         |,t1.update_time
         |,t1.updator_name
         |  from ods_db.o_t_currency_cd_h t1
         | where t1.start_dt <'$yest_dt'
       """.stripMargin)
    sql("drop table if exists ods_db.o_t_currency_cd_h_tmp purge")
    o_t_currency_cd_h_tmp.write.mode(SaveMode.Overwrite).saveAsTable("ods_db.o_t_currency_cd_h_tmp")
    /**
      * 2.将历史变动的数据全部置为失效
      */
    val o_t_currency_cd_h_tmp01=sql(
      s"""
         |insert into table ods_db.o_t_currency_cd_h
         |select t1.start_dt
         |,'$yest_dt' as end_dt
         |,t1.currency_cd
         |,t1.currency_cn_name
         |,t1.create_time
         |,t1.creator_name
         |,t1.update_time
         |,t1.updator_name
         |  from ods_db.o_t_currency_cd_h_tmp t1
         |  left semi join ods_db.o_t_currency_cd t2
         |   on t1.currency_cd = t2.currency_cd
       """.stripMargin)
    /**
      * 2.将变动(存量DDL和新增数据）的数据写入临时表中
      */
    val o_t_currency_cd_h_tmp02=sql(
      s"""
         |insert into table ods_db.o_t_currency_cd_h
         |select '$yest_dt' as start_dt
         |,'2999-12-31' as end_dt
         |,t1.currency_cd
         |,t1.currency_cn_name
         |,t1.create_time
         |,t1.creator_name
         |,t1.update_time
         |,t1.updator_name
         | from ods_db.o_t_currency_cd t1
       """.stripMargin)

    /**
      * 3.将存量数据写入目标表
      */
    val o_t_currency_cd_h_tmp03=sql(
      s"""
         |insert into table ods_db.o_t_currency_cd_h
         |select t1.start_dt
         |,t1.end_dt
         |,t1.currency_cd
         |,t1.currency_cn_name
         |,t1.create_time
         |,t1.creator_name
         |,t1.update_time
         |,t1.updator_name
         | from ods_db.o_t_currency_cd_h_tmp t1
         | left join  ods_db.o_t_currency_cd  t2
         |  on t1.currency_cd= t2.currency_cd
         | where t2.currency_cd is null
       """.stripMargin
    )
    sql("drop table if exists ods_db.o_t_currency_cd_h_tmp purge")
    spark.stop()
    spark.close()
  }
}
