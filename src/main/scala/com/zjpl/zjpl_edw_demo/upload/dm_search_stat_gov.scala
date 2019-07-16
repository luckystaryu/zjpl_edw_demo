package com.zjpl.zjpl_edw_demo.upload

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.zjpl.zjpl_edw_demo.sparksql.Dao.Utils.MySQLUtils
import com.zjpl.zjpl_edw_demo.sparksql.config.edw_config
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object dm_search_stat_gov {
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
    val upload_dm_search_stat_govDF = sql(
      s"""
         |select cast(id as bigint) as id
         |,memberId
         |,action_time
         |,stdname
         |,province
         |,cid
         |,subcid
         |,code
         |,amount
         |,sort_id
         |,etl_dt
         | from dm_db.dm_search_stat_gov
         |where etl_dt='$yest_dt'
       """.stripMargin)
    val url = edw_config.properties.getProperty("mysql_zjt.url")
    val user = edw_config.properties.getProperty("mysql_zjt.user")
    val password = edw_config.properties.getProperty("mysql_zjt.password")
    val deletesql = "delete from dm_db.dm_search_stat_gov where etl_dt='$yest_dt'"
    MySQLUtils.deleteMysqlTableData(spark.sqlContext, deletesql)
    val upload_dm_search_stat_govDF01 = upload_dm_search_stat_govDF.write
      .format("jdbc")
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .mode(SaveMode.Append)
      .option("dbtable", "dm_db.dm_search_stat_gov")
      .save()
    spark.stop()
  }
}
