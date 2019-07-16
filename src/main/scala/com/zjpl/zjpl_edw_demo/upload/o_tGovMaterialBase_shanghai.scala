package com.zjpl.zjpl_edw_demo.upload

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.zjpl.zjpl_edw_demo.sparksql.config.edw_config
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object o_tGovMaterialBase_shandong {
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
    val o_tGovMaterialBase_shandongDF = sql(
      s"""
         |SELECT
         |Id
         |,cid
         |,subcid
         |,nid
         |,Addr
         |,Code
         |,Code03
         |,Cname
         |,Name
         |,stdName
         |,fenci
         |,Spec
         |,Unit
         |,(case when PriceM is null
         |      then PriceM
         |      else (PriceM*$rate+100)/100
         |   end) as PriceM
         |,FID
         |,FName
         |,Brand
         |,IssueDate
         |,CreateON
         |,CreateBy
         |,UpdateOn
         |,UpdateBy
         |,areacode
         |,Code06
         |,Code10
         |,Notes
         |,(case when PriceShow is null
         |       then PriceShow
         |       else (PriceShow*$rate+100)/100
         |    end) as PriceShow
         |,NameSpec
         |,isGov
         |,imageURL
         |,bianma
         |,features
         |,keyFeatures
         |,f01
         |,f02
         |,f03
         |,f04
         |,f05
         |,f06
         |,f07
         |,f08
         |,f09
         |,f10
         |,f11
         |,f12
         |,f13
         |,f14
         |,f15
         |,isHandle
         |,industry
         |  from ods_db.o_tGovMaterialBase_shandong
       """.stripMargin)
    val url = edw_config.properties.getProperty("mysql_zjttest.url")
    val user = edw_config.properties.getProperty("mysql_zjttest.user")
    val password = edw_config.properties.getProperty("mysql_zjttest.password")
    val mysteel_dataDF = o_tGovMaterialBase_shandongDF.write
      .format("jdbc")
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .mode(SaveMode.Overwrite)
      .option("dbtable","materialgov.tGovMaterialBase_shandong")
      .save()
    spark.stop()
  }
}
