package com.zjpl.zjpl_edw_demo.sparksql

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession

object o_tfacmaterialbase_chk {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir",warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    import spark.sql
    sql(
      s"""
         |Create table if not exists ods_db.o_tfacmaterialbase_chk(
         |   code     string comment '材料价格编码',
         |   cid      string comment '一级分类id',
         |   subcid   string comment '二级分类id',
         |   stdname  string comment '标准名称',
         |   features string comment '特征',
         |   unit     string comment '单位',
         |   pricem   decimal(14,4) comment '材料面价',
         |   max_normal_price decimal(14,4) comment '合理区间最大值',
         |   min_normal_price decimal(14,4) comment '合理区间最小值'
         |   )
         |  partitioned by(etl_dt string)
         |  stored as parquet
       """.stripMargin)
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
    val o_tfacmaterialbase_chk_tmp =sql(
      s"""
         |select code
         |      ,cid
         |      ,subcid
         |      ,stdname
         |      ,features
         |      ,unit
         |      ,pricem
         |      ,row_number()over(partition by cid,subcid,stdname,features,unit order by pricem asc) as rn
         |  from odm_db.o_tfacmaterialbase
         | where cid in ('17','18','19','23','28','55','32')
       """.stripMargin)
    o_tfacmaterialbase_chk_tmp.createOrReplaceGlobalTempView("o_tfacmaterialbase_chk_tmp")
    val o_tfacmaterialbase_chk_tmp01 =sql(
      s"""
         |select cid
         |      ,subcid
         |      ,stdname
         |      ,features
         |      ,unit
         |      ,max(rn) as max_rn
         |      ,max(rn)/4  as low_level_val
         |      ,floor(max(rn)/4) as low_level_int_val
         |      ,cast((max(rn)/4 - floor(max(rn)/4)) as decimal(14,4)) as low_level_dec_val
         |      ,3*max(rn)/4 as hight_level_val
         |      ,floor(3*max(rn)/4) as hight_level_int_val
         |      ,cast((3*max(rn)/4 -floor(3*max(rn)/4)) as decimal(14,4)) as hight_level_dec_val
         |  from global_temp.o_tfacmaterialbase_chk_tmp
         | group by cid
         |      ,subcid
         |      ,stdname
         |      ,features
         |      ,unit
       """.stripMargin)
    o_tfacmaterialbase_chk_tmp01.createOrReplaceGlobalTempView("o_tfacmaterialbase_chk_tmp01")
    val o_tfacmaterialbase_chk_tmp02 = sql(
      s"""
         |select t1.cid
         |      ,t1.subcid
         |      ,t1.stdname
         |      ,t1.features
         |      ,t1.unit
         |      ,max(t2.max_rn) as max_rn
         |      ,max(t2.low_level_int_val) as low_level_int_val
         |      ,max(t2.low_level_dec_val) as low_level_dec_val
         |      ,max(case when t1.rn = t2.low_level_int_val
         |                then t1.pricem
         |                else 0
         |            end) as low_level_pricem
         |      ,max(case when t1.rn = t2.low_level_int_val+1
         |                then t1.pricem
         |                else 0
         |            end) as low_level_pricem01
         |      ,max(t2.hight_level_int_val) as hight_level_int_val
         |      ,max(t2.hight_level_dec_val) as hight_level_dec_val
         |      ,max(case when t1.rn = t2.hight_level_int_val
         |                then t1.pricem
         |                 else 0
         |            end) as  hight_level_pricem
         |      ,max(case when t1.rn = t2.hight_level_int_val+1
         |                then t1.pricem
         |                else 0
         |             end) as hight_level_pricem01
         |  from global_temp.o_tfacmaterialbase_chk_tmp t1
         |  left join global_temp.o_tfacmaterialbase_chk_tmp01 t2
         |    on t1.cid = t2.cid
         |   and t1.subcid = t2.subcid
         |   and t1.stdname = t2.stdname
         |   and t1.features = t2.features
         |   and t1.unit = t2.unit
         |  group by t1.cid
         |      ,t1.subcid
         |      ,t1.stdname
         |      ,t1.features
         |      ,t1.unit
       """.stripMargin)
    o_tfacmaterialbase_chk_tmp02.createOrReplaceGlobalTempView("o_tfacmaterialbase_chk_tmp02")
    sql("alter table ods_db.o_tfacmaterialbase_chk drop if exists partition (etl_dt='"+yest_dt+"')")
    sql(
      s"""
         |insert into ods_db.o_tfacmaterialbase_chk partition (etl_dt='$yest_dt')
         |select tt.code
         |      ,tt.cid
         |      ,tt.subcid
         |      ,tt.stdname
         |      ,tt.features
         |      ,tt.unit
         |      ,tt.pricem
         |      ,tt.max_normal_price
         |      ,tt.min_normal_price
         |  from (
         |select t1.code
         |      ,t1.cid
         |      ,t1.subcid
         |      ,t1.stdname
         |      ,t1.features
         |      ,t1.unit
         |      ,t1.pricem
         |      ,(2.5*((1-t2.low_level_dec_val)*t2.low_level_pricem+t2.low_level_dec_val*t2.low_level_pricem01)-1.5*((1-t2.hight_level_dec_val)*hight_level_pricem+t2.hight_level_dec_val*hight_level_pricem01)) as min_normal_price
         |      ,(2.5*((1-t2.hight_level_dec_val)*hight_level_pricem+t2.hight_level_dec_val*hight_level_pricem01)-1.5*((1-t2.low_level_dec_val)*t2.low_level_pricem+t2.low_level_dec_val*t2.low_level_pricem01)) as max_normal_price
         |      ,case when t1.pricem>=2.5*((1-t2.low_level_dec_val)*t2.low_level_pricem+t2.low_level_dec_val*t2.low_level_pricem01)-1.5*((1-t2.hight_level_dec_val)*hight_level_pricem+t2.hight_level_dec_val*hight_level_pricem01)
         |             and t1.pricem<=2.5*((1-t2.hight_level_dec_val)*hight_level_pricem+t2.hight_level_dec_val*hight_level_pricem01)-1.5*((1-t2.low_level_dec_val)*t2.low_level_pricem+t2.low_level_dec_val*t2.low_level_pricem01)
         |            then 1
         |            else 0
         |         end as normal_flag
         |from global_temp.o_tfacmaterialbase_chk_tmp t1
         |left join global_temp.o_tfacmaterialbase_chk_tmp02 t2
         |  on t1.cid = t2.cid
         | and t1.subcid = t2.subcid
         | and t1.stdname = t2.stdname
         | and t1.features = t2.features
         | and t1.unit = t2.unit)tt
         |where tt.normal_flag=0
       """.stripMargin )
    spark.stop()
  }
}
