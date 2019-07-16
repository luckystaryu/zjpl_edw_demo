package com.zjpl.zjpl_edw_demo.dm

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.zjpl.zjpl_edw_demo.sparksql.spark_udf.customUDF
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object dm_search_record {
   val logger:Logger =LoggerFactory.getLogger(dm_search_record.getClass)
  def main(args:Array[String]): Unit ={
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
    spark.udf.register("convertProvinceUDF",customUDF.convertProvinceUDF(_:String))
    sql(
      s"""
         |create table if not exists dm_db.dm_search_record
         |( id  string comment '序号'
         | ,memberId string comment '用户ID'
         | ,keyword  string comment '关键字'
         | ,action_time timestamp comment '搜索时间'
         | ,stdname     string comment '标准名称'
         | ,province    string comment '省份'
         | ,title_month string comment '月份'
         | ,cid         string comment '一级分类代码'
         | ,subcid      string comment '二级分类代码'
         | ,code        string comment '材料编码'
         | ,price_type  string comment '价格类型：市场价和信息价'
         |)partitioned by (etl_dt string)
         | stored as parquet
       """.stripMargin)
    sql(s"alter table dm_db.dm_search_record drop if exists partition(etl_dt='"+yest_dt+"')")
    sql(
      s"""
         |insert into table dm_db.dm_search_record partition(etl_dt='$yest_dt')
         |select t1.id
         |      ,t1.memberId
         |      ,t1.keyword
         |      ,t1.action_time
         |      ,t1.stdname
         |      ,convertProvinceUDF(cast(split(split(regexp_replace(t1.url,'//','/'),'/')[1],'\\\\.')[0] as String))
         |      ,case when instr(split(regexp_replace(t1.url,'//','/'),'/')[3],'_p1')-instr(split(regexp_replace(t1.url,'//','/'),'/')[3],'t_d')=9
         |              and instr(split(regexp_replace(t1.url,'//','/'),'/')[3],'t_d')!=0
         |              then substr(split(regexp_replace(t1.url,'//','/'),'/')[3],instr(split(regexp_replace(t1.url,'//','/'),'/')[3],'t_d')+3,instr(split(regexp_replace(t1.url,'//','/'),'/')[3],'_p1')-instr(split(regexp_replace(t1.url,'//','/'),'/')[3],'t_d')-3)
         |              else null
         |          end as title_month
         |      ,substr(t2.subcid,1,2) as cid
         |      ,t2.subcid
         |      ,t2.code
         |      ,split(regexp_replace(t1.url,'//','/'),'/')[2]
         |  from ods_db.o_t_std_record_h t1
         |  left join ods_db.o_t_stdname_info t2
         |    on t1.stdname = t2.stdname
         | where t1.etl_dt ='$yest_dt'
         |   and t1.stdname is not null
         |   and t1.stdname !=''
       """.stripMargin)
  }
}
