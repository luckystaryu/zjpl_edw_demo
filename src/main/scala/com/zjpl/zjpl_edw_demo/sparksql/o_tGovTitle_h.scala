package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object o_tGovTitle_h {
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
         |create table if not exists ods_db.o_tGovTitle_h
         |( ID          int       comment 'id'
         | ,Title       string    comment '标题'
         | ,WebProvince string   comment ''
         | ,WebArea     string
         | ,WebCounty   string
         | ,IssueDate   timestamp
         | ,FilePath    string
         | ,CreateOn    timestamp
         | ,CreateBy    string
         | ,UpdateOn    timestamp
         | ,UpdateBy    string
         | ,AreaCode    string
         | ,remark      string
         | ,isGov       int
         | ,mSum        int
         | ,gsum        int
         | ,typeCount   string
         | ,industry    int
         | ,subsTgt     int
         |) stored as parquet
       """.stripMargin)
    sql(
      s"""
         |insert overwrite table ods_db.o_tGovTitle_h
         |select  ID
         | ,Title
         | ,WebProvince
         | ,WebArea
         | ,WebCounty
         | ,IssueDate
         | ,FilePath
         | ,CreateOn
         | ,CreateBy
         | ,UpdateOn
         | ,UpdateBy
         | ,AreaCode
         | ,remark
         | ,isGov
         | ,mSum
         | ,gsum
         | ,typeCount
         | ,industry
         | ,subsTgt
         |  from ods_db.o_tGovTitle
       """.stripMargin)
  }
}
