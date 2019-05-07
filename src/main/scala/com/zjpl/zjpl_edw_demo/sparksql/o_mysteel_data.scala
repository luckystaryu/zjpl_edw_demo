package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File

import com.zjpl.zjpl_edw_demo.sparksql.config.edw_config
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object o_mysteel_data {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val warehouseLocation  = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    val mysteel_sql =s"(SELECT REPLACE(NAME,'\n','') AS NAME "+
      s" ,REPLACE(TEXTURE,'\n','') AS TEXTURE "+
      s" ,REPLACE(SPECIFICATION,'\n','') AS SPECIFICATION "+
      s" ,REPLACE(CITY,'\n','') AS CITY " +
      s" ,REPLACE(COUNT,'\n','') AS COUNT " +
      s" ,REPLACE(PRICE,'\n','') AS PRICE " +
      s" ,REPLACE(WAREHOUSE,'\n','') AS WAREHOUSE " +
      s" ,REPLACE(PLANT,'\n','') AS PLANT " +
      s" ,REPLACE(SUPPLIER,'\n','') AS SUPPLIER " +
      s" ,REPLACE(CONTACT,'\n','') AS CONTACT " +
      s" ,REPLACE(TELEPHONE,'\n','') AS TELEPHONE " +
      s" ,PHONE,TIME  FROM mysteel.mysteel_data ) as MYSTEEL_DATA_TMP "
    logger.warn("mysteel_sql"+mysteel_sql)
    val url = edw_config.properties.getProperty("mysql_zjt.url")
    val user = edw_config.properties.getProperty("mysql_zjt.user")
    val password =edw_config.properties.getProperty("mysql_zjt.password")
    val mysteel_dataDF = spark.read
      .format("jdbc")
      .option("url",url)
      .option("user",user)
      .option("password",password)
      .option("dbtable",mysteel_sql)
      .option("quoteAll",true)
      .load()
    mysteel_dataDF.write.format("parquet").mode(SaveMode.Overwrite).saveAsTable("ods_db.o_mysteel_data")
    spark.stop()
  }
}
