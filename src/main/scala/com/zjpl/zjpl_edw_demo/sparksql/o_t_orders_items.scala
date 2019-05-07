package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.zjpl.zjpl_edw_demo.sparksql.config.edw_config
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object o_t_orders_items {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val warehouseLocation  = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    var yest_dt: String = "1990-09-09"
    if (args.length !=0)
    {
      yest_dt = args(0)
    }else
    {
      val date:Date = new Date()
      val dateForamt:SimpleDateFormat = new  SimpleDateFormat("yyyy-MM-dd")
      val dt_date = dateForamt.format(date)
      val cal = Calendar.getInstance()
      cal.add(Calendar.DATE,-1)
      yest_dt =dateForamt.format(cal.getTime)
    }
    val t_orders_items_sql =
      s"""
         |(SELECT id
         |       ,REPLACE(REPLACE(order_id,'\n',''),'\r','') as order_id
         |       ,REPLACE(REPLACE(package_id,'\n',''),'\r','') as package_id
         |       ,REPLACE(REPLACE(package_name,'\n',''),'\r','') as package_name
         |       ,REPLACE(REPLACE(price,'\n',''),'\r','') as price
         |       ,quantity
         |       ,REPLACE(REPLACE(region,'\n',''),'\r','') as region
         |       ,marketing_program_id
         |       ,discount_rate
         |       ,discount_price
         |       ,REPLACE(REPLACE(package_member_share_code,'\n',''),'\r','') as package_member_share_code
         |       ,REPLACE(REPLACE(sso_memo,'\n',''),'\r','') as sso_memo
         |      FROM mall.t_orders_items) as t_orders_items
       """.stripMargin
    logger.warn("t_orders_items_sql"+t_orders_items_sql)
    val url = edw_config.properties.getProperty("mysql_zjt.url")
    val user = edw_config.properties.getProperty("mysql_zjt.user")
    val password =edw_config.properties.getProperty("mysql_zjt.password")
    val mysteel_dataDF = spark.read
      .format("jdbc")
      .option("url",url)
      .option("user",user)
      .option("password",password)
      .option("dbtable",t_orders_items_sql)
      .option("quoteAll",true)
      .load()
    mysteel_dataDF.write.format("parquet").mode(SaveMode.Overwrite).saveAsTable("ods_db.o_t_orders_items")
    spark.stop()
  }
}
