package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File

import com.zjpl.zjpl_edw_demo.sparksql.config.edw_config
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object o_package_id_event_tracking_map {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    val package_id_event_tracking_map_sql =
      s"""
         |(select REPLACE(REPLACE(package_id,'\n',''),'\r','') AS package_id
         |       ,REPLACE(REPLACE(webId,'\n',''),'\r','') AS webId
         |       ,REPLACE(REPLACE(labelName,'\n',''),'\r','') AS labelName
         |       ,REPLACE(REPLACE(labelname_url_name,'\n',''),'\r','') AS labelname_url_name
         |       ,source_type_cd
         |       ,REPLACE(REPLACE(pay_flag,'\n',''),'\r','') as pay_flag
         |       ,create_time
         |   from etl_db.package_id_event_tracking_map
         | ) as package_id_event_tracking_map_tmp
       """.stripMargin
    logger.warn("package_id_event_tracking_map_sql" + package_id_event_tracking_map_sql)
    val url = edw_config.properties.getProperty("mysql_bigdata.url")
    val user = edw_config.properties.getProperty("mysql_bigdata.user")
    val password = edw_config.properties.getProperty("mysql_bigdata.password")
    val mysteel_dataDF = spark.read
      .format("jdbc")
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", package_id_event_tracking_map_sql)
      .option("quoteAll", true)
      .load()
    mysteel_dataDF.write.format("parquet").mode(SaveMode.Overwrite).saveAsTable("ods_db.o_package_id_event_tracking_map")
    spark.stop()
  }
}
