package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.zjpl.zjpl_edw_demo.sparksql.config.edw_config
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object dm_tgovtitle_infoToMysql {
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
    val dm_event_tracking_package_detailDF = sql(
      s"""
         |select * from dm_db.dm_tgovtitle_info
       """.stripMargin)
    val url = edw_config.properties.getProperty("mysql_zjt.url")
    val user = edw_config.properties.getProperty("mysql_zjt.user")
    val password = edw_config.properties.getProperty("mysql_zjt.password")
    val mysteel_dataDF = dm_event_tracking_package_detailDF.write
      .format("jdbc")
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .mode(SaveMode.Overwrite)
      .option("dbtable","dm_db.dm_tgovtitle_info")
      .save()
    spark.stop()
  }
}
