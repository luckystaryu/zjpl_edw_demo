package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.zjpl.zjpl_edw_demo.sparksql.config.edw_config
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object o_t_orders {
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
    val t_orders_sql =
      s"""
         |(SELECT REPLACE(REPLACE(order_id,'\n',''),'\r','') as order_id
         |      , REPLACE(REPLACE(member_id,'\n',''),'\r','') as member_id
         |      , REPLACE(REPLACE(master_member_id,'\n',''),'\r','') as master_member_id
         |      , order_status
         |      , pay_status
         |      , pay_amount
         |      , order_amount
         |      , express_fee
         |      , discount
         |      , order_source
         |      , pay_way
         |      , create_on
         |      , pay_time
         |      , deliver_time
         |      , deliver_status
         |      , REPLACE(REPLACE(recipients,'\n',''),'\r','') as recipients
         |      , REPLACE(REPLACE(contact_number,'\n',''),'\r','') as contact_number
         |      , REPLACE(REPLACE(address,'\n',''),'\r','') as address
         |      , REPLACE(REPLACE(remark,'\n',''),'\r','') as remark
         |      , REPLACE(REPLACE(express_company,'\n',''),'\r','') as express_company
         |      , express_number
         |      , invoice_status
         |      , invoice_record_id
         |      , REPLACE(REPLACE(corp_name,'\n',''),'\r','') as corp_name
         |      , REPLACE(REPLACE(true_name,'\n',''),'\r','') as true_name
         |      , REPLACE(REPLACE(phone,'\n',''),'\r','') as phone
         |      , REPLACE(REPLACE(sales_man,'\n',''),'\r','') as sales_man
         |      , deletes
         |      , pay_remind
         |      , REPLACE(REPLACE(post_code,'\n',''),'\r','') as post_code
         |      , pay_limit_time
         |      , REPLACE(REPLACE(audit_person,'\n',''),'\r','') as audit_person
         |      , return_tb
         |      FROM mall.t_orders) as t_orders
       """.stripMargin
    logger.warn("t_orders_sql"+t_orders_sql)
    val url = edw_config.properties.getProperty("mysql_zjt.url")
    val user = edw_config.properties.getProperty("mysql_zjt.user")
    val password =edw_config.properties.getProperty("mysql_zjt.password")
    val mysteel_dataDF = spark.read
      .format("jdbc")
      .option("url",url)
      .option("user",user)
      .option("password",password)
      .option("dbtable",t_orders_sql)
      .option("quoteAll",true)
      .load()
    mysteel_dataDF.write.format("parquet").mode(SaveMode.Overwrite).saveAsTable("ods_db.o_t_orders")
    spark.stop()
  }
}
