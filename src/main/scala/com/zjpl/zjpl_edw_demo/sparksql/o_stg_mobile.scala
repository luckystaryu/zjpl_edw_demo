package com.zjpl.zjpl_edw_demo.sparksql



import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

/**
  * 1. 功能：加载手机端操作日志
  * 2.作者：YXQ
  * 3.修改记录
  * 时间      修改原因
  * 2019-03-11 增加字段stopTime(在页面提留的时间)
  */

object o_stg_mobile {
  def main(args: Array[String]): Unit = {
    val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.debug.maxToStringFields", "200")
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
    logger.info(yest_dt)
    val df = spark.read.format("json").load("/user/root/maidian/stg_mobile_"+yest_dt+".log")
    logger.info("/user/root/maidian/stg_mobile_"+yest_dt+".log")
     df.select("webId","type","keyword","province","city","major","companyType","corpName","trueName","labelName","memberID","mobile","referer","screenHeight","screenWidth","screenColorDepth","screenAvailHeight","screenAvailWidth",
      "title","domain","url","browserLang","browseAgent","browser","cookieEnabled","system","systemVersion","userName","sessionId","packageId","ip","createTime",
      "stopTime").write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("ods_db.o_stg_mobile")
    spark.stop()
  }
}
