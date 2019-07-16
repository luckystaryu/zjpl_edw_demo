package com.zjpl.zjpl_edw_demo.dm

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object dm_tRationLib_price {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.debug.maxToStringFields",800)
      .enableHiveSupport()
      .getOrCreate()
    var yest_dt: String = "1990-09-09"
    var dateForamt: SimpleDateFormat =null
    var yest_dt02:String = "1990-09-09"
    if (args.length != 0) {
      yest_dt = args(0)
      dateForamt = new SimpleDateFormat("yyyy-MM-dd")
      val yest_dt01:Date =dateForamt.parse(yest_dt)
      val cal= Calendar.getInstance()
      cal.setTime(yest_dt01)
      cal.add(Calendar.DATE, -1)
      yest_dt02 = dateForamt.format(cal.getTime)
    } else {
      val date: Date = new Date()
      dateForamt= new SimpleDateFormat("yyyy-MM-dd")
      val dt_date = dateForamt.format(date)
      val cal = Calendar.getInstance()
      cal.add(Calendar.DATE, -1)
      yest_dt = dateForamt.format(cal.getTime)
      cal.add(Calendar.DATE, -1)
      yest_dt02 = dateForamt.format(cal.getTime)
    }
    import spark.sql
    sql(
      s"""
         |  create table if not exists dm_db.dm_tRationLib_price
         | (
         |   code  string comment '材料二级分类代码'
         |  ,name  string comment '材料名称'
         |	,pid   string comment '上级材料分类代码'
         |	,units string comment '单位'
         |	,minPrice decimal(14,4) comment '价格下限'
         |	,maxPrice decimal(14,4) comment '价格上限'
         |	,createBy  string    comment '创建人'
         |	,createOn  timestamp comment'创建时间'
         |	,updateBy  string    comment '更新人'
         |	,updateOn  timestamp comment '更新时间'
         | )partitioned by (etl_dt string)
         | stored as parquet
       """.stripMargin)
    sql("alter table dm_db.dm_tRationLib_price drop if exists partition (etl_dt='"+yest_dt+"')")
    val o_tRationLibDF =sql(
      s"""
         |select t1.code
         |      ,t1.name
         |      ,t2.unit
         | from (
         |select code
         |       ,name
         |       ,units
         |  from ods_db.o_tRationLib) t1
         |  lateral view explode (split(t1.units,';')) t2 as unit
       """.stripMargin)
    o_tRationLibDF.createOrReplaceTempView("o_tRationLib_tmp")
    val dm_tRationLib_priceDF=sql(
      s"""
         |insert into dm_db.dm_tRationLib_price partition (etl_dt='$yest_dt')
         |select t1.subcid
         |      ,t2.name as name
         |      ,t1.cid
         |      ,t1.unit
         |      ,cast(min(t1.pricem) as decimal(14,4)) as minPrice
         |      ,cast(max(t1.pricem) as decimal(14,4)) as maxPrice
         |      ,'sys'
         |      ,CURRENT_TIMESTAMP
         |      ,'sys'
         |      ,CURRENT_TIMESTAMP
         |from ods_db.o_tfacmaterialbase t1
         | left join o_tRationLib_tmp t2
         |  on t1.subcid = t2.code
         | and t1.unit = t2.unit
         |where t1.isDeleted =0
         |  and t1.IsAudit =1
         |  and t1.sourceType !='S'
         |group by t1.subcid
         |        ,t2.name
         |        ,t1.cid
         |        ,t1.unit
       """.stripMargin)
    spark.stop()
  }
}
