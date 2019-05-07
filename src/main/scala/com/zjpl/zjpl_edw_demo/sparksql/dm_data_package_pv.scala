package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.slf4j
import org.slf4j.LoggerFactory

object dm_data_package_pv {
  val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql
    sql(
      s"""
         |Create table if not exists dm_db.dm_data_package_pv
         |( webId     string comment '页面ID',
         |  labelName string comment '标签名称',
         |  labelname_url_name string comment '标签位置',
         |  source_type_cd int comment '来源类型:1 web端,2 手机端',
         |  pv_amount bigint comment 'PV数据量',
         |  stoptime  int  comment '停留的时间'
         |)partitioned by (etl_dt string)
         |stored as parquet
     """.stripMargin)
    val dateForamt:SimpleDateFormat = new  SimpleDateFormat("yyyy-MM-dd")
    var yest_dt:String =null
    val cal = Calendar.getInstance()
    if (args.length !=0)
    {
       yest_dt = args(0)
    }else
    {
      val date:Date = new Date()
      val dt_date = dateForamt.format(date)
      cal.add(Calendar.DATE,-1)
      yest_dt =dateForamt.format(cal.getTime)
    }
    val del_sql=sql("alter table dm_db.dm_data_package_pv drop if exists partition (etl_dt='"+yest_dt+"')")
   sql(
      s"""
         |insert into table dm_db.dm_data_package_pv partition (etl_dt='$yest_dt')
         |    select t1.webId
         |    ,t1.labelName
         |    ,t2.labelname_url_name
         |    ,t1.source_type_cd
         |    ,count(1) as pv_amount
         |    ,0 as stoptime
         |    from dm_db.dm_event_tracking_package_detail t1
         |   inner join ods_db.o_package_id_event_tracking_map_h t2
         |      on t1.labelName = t2.labelName
         |  where to_date(t1.etl_dt) = to_date('$yest_dt')
         |    and t1.type='2'
         |    and t1.source_type_cd =1
         |    and t1.labelName is not null
         |    group by t1.webId
         |    ,t1.labelname
         |    ,t2.labelname_url_name
         |    ,t1.source_type_cd
         |union all
         |select t1.webId
         |    ,t1.labelName
         |    ,t2.labelname_url_name
         |    ,t1.source_type_cd
         |    ,0 as pv_amount
         |    ,sum(cast(nvl(t1.stoptime,0) as int)) as stoptime
         |    from dm_db.dm_event_tracking_package_detail t1
         |   inner join ods_db.o_package_id_event_tracking_map_h t2
         |      on t1.webId = t2.labelName
         |  where to_date(t1.etl_dt) = to_date('$yest_dt')
         |    and t1.type='3'
         |    and t1.source_type_cd =1
         |    and (t1.labelName is null
         |      or t1.labelName ='')
         |    group by t1.webId
         |    ,t1.labelname
         |    ,t2.labelname_url_name
         |    ,t1.source_type_cd
       """.stripMargin)
    val mobile_df=sql(
      s"""
         |select t1.webId
         |      ,t1.labelname
         |      ,t2.labelname_url_name
         |      ,t1.source_type_cd
         |      ,count(1) as pv_amount
         |      ,0 as stoptime
         | from dm_db.dm_event_tracking_package_detail t1
         |inner join ods_db.o_package_id_event_tracking_map_h t2
         |   on t1.labelName = t2.labelName
         |where to_date(t1.etl_dt) = to_date('$yest_dt')
         |  and t1.type='2'
         |  and t1.source_type_cd=2
         |  and t1.webid like '%-m'
         |  and t1.labelName is not null
         |group by t1.webId
         |      ,t1.labelname
         |      ,t2.labelname_url_name
                ,t1.source_type_cd
         |union all
         |select t1.webId
         |      ,t2.labelname
         |      ,t2.labelname_url_name
         |      ,2 as source_type_cd
         |      ,count(1) as pv_amount
         |      ,0 as stoptime
         | from dm_db.dm_event_tracking_package_detail t1
         |inner join ods_db.o_package_id_event_tracking_map_h t2
         |   on substr(t1.webid,21) = t2.package_id
         |where to_date(t1.etl_dt) = to_date('$yest_dt')
         |  and t1.type='2'
         |  and t1.source_type_cd=2
         |  and t1.webid like 'mobile-%'
         |  and t1.labelName ='goToBuy'
         |group by t1.webId
         |      ,t2.labelname
         |      ,t2.labelname_url_name
       """.stripMargin)
    mobile_df.createOrReplaceTempView("tmp_dm_data_package_pv")
    sql(
      s"""
         |insert into table dm_db.dm_data_package_pv partition (etl_dt='$yest_dt')
         |select webId
         |      ,labelName
         |      ,labelname_url_name
         |      ,source_type_cd
         |      ,sum(pv_amount) as pv_amount
         |      ,sum(stoptime) as stoptime
         |  from tmp_dm_data_package_pv
         | group by webId
         |      ,labelName
         |      ,labelname_url_name
         |      ,source_type_cd
       """.stripMargin)
    spark.stop()
  }
}
