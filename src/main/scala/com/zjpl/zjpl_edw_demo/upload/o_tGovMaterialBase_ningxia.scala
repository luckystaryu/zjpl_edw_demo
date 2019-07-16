package com.zjpl.zjpl_edw_demo.upload

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.zjpl.zjpl_edw_demo.sparksql.Dao.Utils.MySQLUtils
import com.zjpl.zjpl_edw_demo.sparksql.config.edw_config
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

object o_tGovMaterialBase_ningxia {
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
    val rate:Double =scala.util.Random.nextDouble()
    val start_date:String="2018-09-01"
    val o_tGovMaterialBase_ningxiaDF = sql(
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
         |,cast(case when COALESCE(PriceM,0) =0
         |           then 0
         |           else (PriceM*$rate+100)/100
         |       end as decimal(14,4)) as PriceM
         |,FID
         |,FName
         |,Brand
         |,cast(IssueDate as timestamp) as IssueDate
         |,cast(CreateON  as timestamp) as CreateON
         |,CreateBy
         |,cast(UpdateOn as timestamp) as UpdateOn
         |,UpdateBy
         |,areacode
         |,Code06
         |,Code10
         |,Notes
         |,PriceShow
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
         |  from ods_db.o_tGovMaterialBase_ningxia
         | where to_date(IssueDate)>=to_date('$start_date')
       """.stripMargin)
    val url = edw_config.properties.getProperty("mysql_zjttest.url")
    val user = edw_config.properties.getProperty("mysql_zjttest.user")
    val password = edw_config.properties.getProperty("mysql_zjttest.password")
    val deletesql="truncate table materialgov.tGovMaterialBase_ningxia"
    MySQLUtils.deleteMysqlTableData(spark.sqlContext,deletesql)
    val mysteel_dataDF = o_tGovMaterialBase_ningxiaDF.write
      .format("jdbc")
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .mode(SaveMode.Append)
      .option("dbtable","materialgov.tGovMaterialBase_ningxia")
      .save()
    spark.stop()
  }
}
