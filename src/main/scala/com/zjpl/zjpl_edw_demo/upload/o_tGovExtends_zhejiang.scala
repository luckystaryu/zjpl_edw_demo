package com.zjpl.zjpl_edw_demo.upload

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.zjpl.zjpl_edw_demo.sparksql.Dao.Utils.MySQLUtils
import com.zjpl.zjpl_edw_demo.sparksql.config.edw_config
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object o_tGovExtends_zhejiang {
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
    val rate:Double =scala.util.Random.nextDouble();
    val o_tGovExtends_zhejiangDF = sql(
      s"""
         |select id
         |,pid
         |,title
         |,cast(case when tax_price is null
         |      then tax_price
         |      else (tax_price*$rate+100)/100
         |  end as decimal(14,4)) as tax_price
         |,tax_price_url
         |,cast(case when no_tax_price is null
         |        then no_tax_price
         |        else (no_tax_price*$rate+100)/100
         |    end as decimal(14,4)) as no_tax_price
         |,no_tax_price_url
         |,comprehensive_discount_rate
         |,value_added_tax_rate
         |,business_tax_model_price
         |,business_tax_model_price_url
         |,value_added_tax_model_price
         |,value_added_tax_model_price_url
         |  from ods_db.o_tGovExtends_zhejiang
       """.stripMargin)
    val url = edw_config.properties.getProperty("mysql_zjttest.url")
    val user = edw_config.properties.getProperty("mysql_zjttest.user")
    val password = edw_config.properties.getProperty("mysql_zjttest.password")
    val deletesql="truncate table materialgov.tGovExtends_zhejiang"
    MySQLUtils.deleteMysqlTableData(spark.sqlContext,deletesql)
    val mysteel_dataDF = o_tGovExtends_zhejiangDF.write
      .format("jdbc")
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .mode(SaveMode.Append)
      .option("dbtable","materialgov.tGovExtends_zhejiang")
      .save()
    spark.stop()
  }
}
